use std::collections::HashMap;
use serde::Deserialize;

/// WARC レコードの汎用構造
#[derive(Debug)]
pub struct WarcRecord {
    pub version: String,
    pub headers: HashMap<String, String>,
    pub content: Vec<u8>,
}

/// メタデータレコード
#[derive(Debug)]
pub struct MetadataRecord {
    pub record: WarcRecord,
    pub fetch_time_ms: Option<u64>,
    pub charset_detected: Option<String>,
    pub languages_cld2: Option<CLD2Result>,
}

/// HTML レスポンスレコード
#[derive(Debug)]
pub struct HtmlRecord {
    pub record: WarcRecord,
    pub html: String,
}

/// CLD2 言語検出結果
#[derive(Debug, Deserialize)]
pub struct CLD2Result {
    pub reliable: bool,
    #[serde(rename = "text-bytes")]
    pub text_bytes: usize,
    pub languages: Vec<LangInfo>,
}

/// 各言語情報
#[derive(Debug, Deserialize)]
pub struct LangInfo {
    pub code: String,
    #[serde(rename = "code-iso-639-3")]
    pub code_iso_639_3: String,
    #[serde(rename = "text-covered")]
    pub text_covered: f64,
    pub score: f64,
    pub name: String,
}
