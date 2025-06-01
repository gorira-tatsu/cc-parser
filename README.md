# cc-parser

This repository contains a simple Rust program for parsing Common Crawl WARC files.

## Test Data

The `cc-data/warc/sample.warc` file provides a minimal concatenated WARC with two
response records. Each record includes a small HTML snippet served from an
example domain. The content is intentionally simple and does not contain any
copyrighted or restricted data.

You can use this file to run local tests of the parser without downloading
external resources.
