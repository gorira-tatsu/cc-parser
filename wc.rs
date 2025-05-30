use std::collections::HashMap;
use std::env;
use std::fs;

fn main() {
    // Expect the first argument to be the text file path.
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <text_file>", args[0]);
        std::process::exit(1);
    }
    let filename = &args[1];

    // Read file contents.
    let contents = fs::read_to_string(filename)
        .expect("Failed to read file");

    // Count word frequencies.
    let mut word_counts = HashMap::new();
    for word in contents.split_whitespace() {
        // Normalize: lowercase and remove surrounding non-alphanumeric characters.
        let word = word
            .to_lowercase()
            .trim_matches(|c: char| !c.is_alphanumeric());
        if word.is_empty() {
            continue;
        }
        *word_counts.entry(word.to_string()).or_insert(0) += 1;
    }

    // Convert counts to a vector and sort by frequency in descending order.
    let mut words: Vec<(String, usize)> = word_counts.into_iter().collect();
    words.sort_by(|a, b| b.1.cmp(&a.1));

    // Display words as a simple word crowd:
    // The number of '#' indicates the word frequency.
    for (word, count) in words {
        println!("{:<15} {}", word, "#".repeat(count));
    }
}