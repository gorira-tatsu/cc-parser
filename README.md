# cc-parser

This repository contains a simple Rust program for parsing Common Crawl WARC files.

## Test Data

The `testdata/sample.warc` file provides a small concatenated WARC with four
response records. Two records are short HTML snippets from example domains in
English. The other two include Japanese text to better simulate real pages.
All content was written for testing purposes so that no licensed material is
included.

You can use this file to run local tests of the parser without downloading
external resources.
