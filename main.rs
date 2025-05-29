use std::fs;
use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::time::{Instant, Duration};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Result, Write};
use url::Url;
use warc::WarcReader;
use warc::WarcHeader;
use rayon::prelude::*;
use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};
use html5ever::tendril::TendrilSink;
use html5ever::parse_document;
use markup5ever_rcdom::{RcDom, Handle, NodeData};
use whatlang::{detect, Lang};

/// build a global set of blocked domains from each subfolder’s `domains` file
static BLOCKED_DOMAINS: Lazy<HashSet<String>> = Lazy::new(|| {
    let mut set = HashSet::new();
    // iterate every entry in project root
    for entry in fs::read_dir("./ut1_blocklist").unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            let dom = entry.path().join("domains");
            if dom.exists() {
                if let Ok(txt) = fs::read_to_string(&dom) {
                    for line in txt.lines() {
                        let l = line.trim();
                        if !l.is_empty() && !l.starts_with('#') {
                            set.insert(l.to_string());
                        }
                    }
                }
            }
        }
    }
    set
});

const MAX_RECORDS_PER_FILE: usize = 0; // set >0 to limit records per file
const PROGRESS_INTERVAL: usize = 1000; // log progress every 100 records
const DETECT_PREFIX_CHARS: usize = 512; // max characters for language detection (increased to catch Japanese)
const LONG_SENTENCE_LEN: usize = 100; // threshold for 'long' sentences

// n-gram 設定
const NGRAM_SIZE: usize = 3;
const NGRAM_REPEAT_THRESHOLD: usize = 10;

// Compile regexes once
static LANG_REGEX: Lazy<Regex> = Lazy::new(|| {
    RegexBuilder::new(r#"<html\b[^>]*\blang=['"]?([a-zA-Z-]+)['"]?"#)
        .case_insensitive(true)
        .build()
        .unwrap()
});
static HIRA_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\u3040-\u309F]").unwrap());
static KATA_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\u30A0-\u30FF]").unwrap());
static CJK_REGEX:  Lazy<Regex> = Lazy::new(|| Regex::new(r"[\u4E00-\u9FFF]").unwrap());
static DATE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\d{4}年\d{1,2}月").unwrap()
});

const MONTH_LIST_THRESHOLD: usize = 5;

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
    let has_hira = HIRA_REGEX.is_match(content);
    let has_kata = KATA_REGEX.is_match(content);
    let has_cjk  = CJK_REGEX.is_match(content);
    // require at least two of the three scripts
    [has_hira, has_kata, has_cjk].iter().filter(|&&b| b).count() >= 2
}

/// Strip HTML tags using html5ever+RcDom, skipping script/style content
/// and normalize whitespace in one pass.
fn strip_tags(input: &str) -> String {
    let dom: RcDom = parse_document(RcDom::default(), Default::default()).one(input);
    fn recurse(handle: &Handle, out: &mut String, prev_space: &mut bool) {
        if let NodeData::Element { name, .. } = &handle.data {
            let tag = name.local.as_ref();
            if tag.eq_ignore_ascii_case("script")
                || tag.eq_ignore_ascii_case("style")
                || tag.eq_ignore_ascii_case("header")
                || tag.eq_ignore_ascii_case("footer")
                || tag.eq_ignore_ascii_case("nav")
            {
                return;
            }
        }
        if let NodeData::Text { contents } = &handle.data {
            for ch in contents.borrow().chars() {
                if ch.is_whitespace() {
                    if !*prev_space {
                        out.push(' ');
                        *prev_space = true;
                    }
                } else {
                    out.push(ch);
                    *prev_space = false;
                }
            }
        }
        for child in handle.children.borrow().iter() {
            recurse(child, out, prev_space);
        }
    }
    let mut text = String::new();
    let mut prev_space = true;
    recurse(&dom.document, &mut text, &mut prev_space);
    text.trim().to_string()
}


pub fn has_repeating_ngrams(text: &str, n: usize, threshold: usize) -> bool {
    // 1. 事前処理（小文字化、句読点・記号の除去など）
    let normalized = text
        .to_lowercase()
        .replace(|c: char| !c.is_alphanumeric() && !c.is_whitespace(), "");

    // 2. 単語単位 (word-based) の n-gram
    let words: Vec<&str> = normalized.split_whitespace().collect();
    if check_ngram_count(&words, n, threshold) {
        return true;
    }

    // 3. 文字単位 (character-based) の n-gram
    //   全角文字や結合文字を考慮する場合は別途対応が必要
    //   ここでは単純な例として chars() を使う
    let chars: Vec<char> = normalized.chars().collect();
    if check_ngram_count(&chars, n, threshold) {
        return true;
    }

    false
}

fn check_ngram_count<T: Eq + std::hash::Hash + Clone>(
    tokens: &[T],
    n: usize,
    threshold: usize,
) -> bool {
    let mut counts = HashMap::new();
    for window in tokens.windows(n) {
        // 簡易的にコピーして集合キーを作成
        let key: Vec<T> = window.to_vec();
        let cnt = counts.entry(key).or_insert(0);
        *cnt += 1;
        if *cnt > threshold {
            return true;
        }
    }
    false
}

/// Process and filter text. Returns Some(cleaned_text) if record should be kept, None otherwise.
fn process_text(
    text: &str,
    detect_time: &mut Duration,
    tag_time: &mut Duration,
    _filter_time: &mut Duration,
) -> Option<String> {
    // prefilter by HTML lang & Japanese scripts
    if !is_japanese_page_by_lang_regexp(text) || !contains_japanese_text(text) {
        return None;
    }

    // タグ除去
    let extracted = strip_tags(text);

    // 年月リストが多すぎるパターンを除外
    let date_count = DATE_REGEX.find_iter(&extracted).count();
    if date_count > MONTH_LIST_THRESHOLD {
        return None;
    }

    // n-gram 重複チェック
    if has_repeating_ngrams(&extracted, NGRAM_SIZE, NGRAM_REPEAT_THRESHOLD) {
        println!("Skipping due to repeating n-grams");
        return None;
    }

    // ※ split_whitespace().join(" ") は strip_tags 内で済んでいるので削除
    let extracted = extracted;

    // 言語検出 (既存)
    let prefix: String = extracted.chars().take(DETECT_PREFIX_CHARS).collect();
    let dt_start = Instant::now();
    let is_jpn = matches!(detect(&prefix), Some(info) if info.lang() == Lang::Jpn);
    *detect_time += dt_start.elapsed();
    if !is_jpn {
        return None;
    }

    Some(extracted)
}

fn process_wet_file(path: &str) -> Result<()> {
    println!("--- Processing {} ---", path);
    
    // Create consolidated HTML output file for this WET file
    let file_stem = Path::new(path).file_stem().unwrap().to_string_lossy();
    let html_filename = format!("japanese_html_{}.txt", file_stem);
    let mut html_output = OpenOptions::new().create(true).write(true).truncate(true).open(&html_filename)?;
    println!("Saving Japanese HTML content to {}", html_filename);
    
    println!("Reading records (progress every {} records)...", PROGRESS_INTERVAL);
    std::io::stdout().flush().unwrap();
    let file_start = Instant::now();
    // Performance timers
    let mut total_detect_time = Duration::ZERO;
    let mut total_tag_time    = Duration::ZERO;
    let mut total_filter_time = Duration::ZERO;
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
            // host-based skip
            let uri = rec.header(WarcHeader::TargetURI).unwrap_or_default();
            if let Ok(parsed) = Url::parse(uri.as_ref()) {
                if let Some(host) = parsed.host_str() {
                    if BLOCKED_DOMAINS.contains(host) {
                        continue;
                    }
                }
            }

            // extract HTTP headers and body
            let body_bytes = rec.body();
            let body_str = match std::str::from_utf8(body_bytes) {
                Ok(s) => s,
                Err(_) => continue,
            };
            // split headers and payload
            let (hdr, payload) = if let Some(i) = body_str.find("\r\n\r\n") {
                (&body_str[..i], &body_str[i + 4..])
            } else if let Some(i) = body_str.find("\n\n") {
                (&body_str[..i], &body_str[i + 2..])
            } else {
                continue;
            };

            let is_response = rec.header(WarcHeader::WarcType)
                .map_or(false, |wt| wt == "response");
            let has_application_http = rec.header(WarcHeader::ContentType)
                .map_or(false, |ct| ct.contains("application/http"));
            if !is_response || !has_application_http {
                continue;
            }

            // now payload is the HTML content
            let text = payload;

             // Process and write cleaned text
             if let Some(cleaned) = process_text(
                &text,
                &mut total_detect_time,
                &mut total_tag_time,
                &mut total_filter_time,
            ) {
                kept_count += 1;
                // write WARC metadata (including Content-Length)
                let wt  = rec.header(WarcHeader::WarcType).unwrap_or_default();
                let uri = rec.header(WarcHeader::TargetURI).unwrap_or_default();
                let cl  = rec.header(WarcHeader::ContentLength).unwrap_or_default();
                let ct  = rec.header(WarcHeader::ContentType).unwrap_or_default();
                html_output.write_all(format!(
                    "WARC-Type: {}\nWARC-Target-URI: {}\nWARC-Content-Length: {}\nWARC-Content-Type: {}\n\n",
                    wt.as_ref(), uri.as_ref(), cl.as_ref(), ct.as_ref()
                ).as_bytes()).unwrap();
                // write cleaned text and boundary
                html_output.write_all(cleaned.as_bytes()).unwrap();
                html_output.write_all(b"\n\n--- RECORD BOUNDARY ---\n\n").unwrap();
            }
        } else if let Err(e) = record_result {
            eprintln!("Error reading record in {}: {}", path, e);
        }
    }
    let elapsed = file_start.elapsed();
    println!("Kept {} records for {} (out of {})", kept_count, path, record_count);
    println!("Processed {} records in {:.2?}", record_count, elapsed);
    // Report detailed performance
    println!("Total detect time: {:.2?} ({:.1}% of total)", total_detect_time,
             total_detect_time.as_secs_f64()/elapsed.as_secs_f64()*100.0);
    println!(
        "Total TAG removal time: {:.2?} ({:.1}% of total)",
        total_tag_time,
        total_tag_time.as_secs_f64() / elapsed.as_secs_f64() * 100.0
    );
    println!(
        "Total sentence-filter time: {:.2?} ({:.1}% of total)",
        total_filter_time,
        total_filter_time.as_secs_f64() / elapsed.as_secs_f64() * 100.0
    );
    // process_text 全体の合計時間と割合
    let total_process_text = total_detect_time + total_tag_time + total_filter_time;
    println!(
        "Total process_text time: {:.2?} ({:.1}% of total)",
        total_process_text,
        total_process_text.as_secs_f64() / elapsed.as_secs_f64() * 100.0
    );

    Ok(())
}

fn main() -> Result<()> {
    // Print loaded blocklist size for verification
    println!("Loaded blocked domains: {}", BLOCKED_DOMAINS.len());

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
