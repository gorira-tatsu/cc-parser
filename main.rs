use std::fs::{self, File, OpenOptions};
use std::path::Path;
use std::io::{BufReader, Result, Write};
use std::time::{Instant, Duration};
use warc::WarcReader;
use whatlang::{detect, Lang};
use rayon::prelude::*;
use once_cell::sync::Lazy;
use regex::RegexBuilder;
use regex::Regex;

const MAX_RECORDS_PER_FILE: usize = 1000; // set >0 to limit records per file
const PROGRESS_INTERVAL: usize = 1000; // log progress every 100 records
const DETECT_PREFIX_CHARS: usize = 512; // max characters for language detection (increased to catch Japanese)
const LONG_SENTENCE_LEN: usize = 100; // threshold for 'long' sentences

// Compile regexes once
static LANG_REGEX: Lazy<Regex> = Lazy::new(|| {
    RegexBuilder::new(r#"<html\b[^>]*\blang=['"]?([a-zA-Z-]+)['"]?"#)
        .case_insensitive(true)
        .build()
        .unwrap()
});
static JP_TEXT_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\u3040-\u30FF\u4E00-\u9FFF]").unwrap());

/// Check <html lang=...> attribute for 'ja'
fn is_japanese_page_by_lang_regexp(content: &str) -> bool {
    if let Some(caps) = LANG_REGEX.captures(content) {
        if let Some(m) = caps.get(1) {
            // only allow if lang starts with 'ja'
            return m.as_str().to_lowercase().starts_with("ja");
        }
    }
    // no lang attribute: do not filter out
    true
}

/// Fast check for any Japanese text characters
fn contains_japanese_text(content: &str) -> bool {
    JP_TEXT_REGEX.is_match(content)
}

/// Process and filter text. Returns true if record should be kept (Japanese + long sentence), and dumps raw sample.
fn process_text(
    text: &str,
    path: &str,
    record_count: usize,
    raw_dump: &mut File,
    detect_time: &mut Duration,
) -> bool {
    // Fast prefilter by HTML lang attribute and Japanese text presence
    if !is_japanese_page_by_lang_regexp(text) || !contains_japanese_text(text) {
        return false;
    }

    // Language detection on prefix
    let prefix: String = text.chars().take(DETECT_PREFIX_CHARS).collect();
    let dt_start = Instant::now();
    let is_jpn = detect(&prefix).map_or(false, |info| info.lang() == Lang::Jpn);
    *detect_time += dt_start.elapsed();
    if !is_jpn {
        return false;
    }
    // Long sentence check
    let has_long = text.split('。')
        .map(str::trim)
        .any(|seg| seg.chars().count() > LONG_SENTENCE_LEN && seg.contains('、'));
    if !has_long {
        return false;
    }
    // Dump raw 500 chars
    let raw_sample: String = text.chars().take(500).collect();
    writeln!(raw_dump, "--- Raw 500-char sample from {} record {} ---", path, record_count).ok();
    writeln!(raw_dump, "{}", raw_sample).ok();
    writeln!(raw_dump, "--- End sample ---\n").ok();
    // flush to guarantee the dump is written immediately
    raw_dump.flush().unwrap();
    true
}

fn process_wet_file(path: &str) -> Result<()> {
    println!("--- Processing {} ---", path);
    // Open raw sample dump file once per WET file, with filename based on the WET file stem
    let dump_filename = format!("raw_samples_{}.txt", Path::new(path)
        .file_stem().unwrap().to_string_lossy());
    println!("Dumping raw samples to {}", dump_filename);
    let mut raw_dump = OpenOptions::new().create(true).append(true).open(&dump_filename)?;
    println!("Reading records (progress every {} records)...", PROGRESS_INTERVAL);
    std::io::stdout().flush().unwrap();
    let file_start = Instant::now();
    // Performance timers
    let mut total_decode_time = Duration::ZERO;
    let mut total_detect_time = Duration::ZERO;
    let mut total_process_time = Duration::ZERO;
    let mut record_count = 0;
    let mut kept_count = 0; // count of records that passed filters

    let file = File::open(path)?;
    let reader = WarcReader::new(BufReader::new(file));
    for record_result in reader.iter_records() {
        record_count += 1;
        // Progress monitoring
        if PROGRESS_INTERVAL > 0 && record_count % PROGRESS_INTERVAL == 0 {
            let elapsed = file_start.elapsed();
            println!("… {} records processed ({:.2?}) …", record_count, elapsed);
            std::io::stdout().flush().unwrap();
        }
        // Optional record limit
        if MAX_RECORDS_PER_FILE > 0 && record_count >= MAX_RECORDS_PER_FILE {
            println!("Record limit {} reached, stopping.", MAX_RECORDS_PER_FILE);
            break;
        }
        if let Ok(rec) = record_result {
            // Decode body to text and time it
            let decode_start = Instant::now();
            let body = rec.body();
            let text = String::from_utf8_lossy(body);
            total_decode_time += decode_start.elapsed();

            // Process and filter text
            let pp_start = Instant::now();
            let keep = process_text(&text, path, record_count, &mut raw_dump, &mut total_detect_time);
            total_process_time += pp_start.elapsed();
            if keep {
                kept_count += 1;
            } else {
                continue;
            }
        } else if let Err(e) = record_result {
            eprintln!("Error reading record in {}: {}", path, e);
        }
    }
    let elapsed = file_start.elapsed();
    println!("Kept {} records for {} (out of {})", kept_count, path, record_count);
    println!("Processed {} records in {:.2?}", record_count, elapsed);
    // Report detailed performance
    println!("Total decode time: {:.2?} ({:.1}% of total)", total_decode_time,
             total_decode_time.as_secs_f64()/elapsed.as_secs_f64()*100.0);
    println!("Total process_text time: {:.2?} ({:.1}% of total)", total_process_time,
             total_process_time.as_secs_f64()/elapsed.as_secs_f64()*100.0);
    println!("Total detect time: {:.2?} ({:.1}% of total)", total_detect_time,
             total_detect_time.as_secs_f64()/elapsed.as_secs_f64()*100.0);
    Ok(())
}

fn main() -> Result<()> {
    let dir = "output-warc";
    // Collect all .wet file paths
    let paths: Vec<String> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("warc"))
        .filter_map(|p| p.to_str().map(|s| s.to_string()))
        .collect();

    // Process files in parallel
    paths.par_iter().for_each(|path| {
        if let Err(e) = process_wet_file(path) {
            eprintln!("Error processing {}: {}", path, e);
        }
    });
    Ok(())
}
