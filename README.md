JSON Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-json/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-json/actions/workflows/maven.yml)
==========================

## Overview

JSON Data Store is an extension for Fess Data Store Crawling. It reads JSON files
from the local file system and indexes each JSON object as a document.

Three document shapes are supported, and by default the shape is detected from the
document itself:

- JSON Lines - one JSON object per line
- a JSON array of objects, pretty-printed or minified
- a single JSON object

Records are pulled one at a time, so a large array is not held in memory. Remote
sources are not supported: the `urls` parameter is rejected rather than ignored.

## Installation

Install it from the Fess admin UI, under System > Plugin, or place the jar
manually:

1. Download `fess-ds-json-X.X.X.jar` from the
   [CodeLibs repository](https://maven.codelibs.org/release/org/codelibs/fess/fess-ds-json/)
   (15.7.0 and earlier are on
   [Maven Central](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-json/)).
2. Copy it to `$FESS_HOME/app/WEB-INF/plugin` (`/usr/share/fess/app/WEB-INF/plugin`
   for a package install).
3. Restart Fess.

## Crawling Setting

Create the configuration under Crawler > Data Store, with `JsonDataStore` as the
handler name.

Parameter:

```
files=/var/data/products.jsonl
file_encoding=UTF-8
```

Script:

```
url="https://shop.example.com/product/" + id
title=name
content=description
digest=description
host="shop.example.com"
site="shop.example.com"
```

Top-level fields of the record are readable in the script as variables of the same
name. A nested object is a map and a nested array is a list, so `product.name` and
`tags[0]` work as well. A field whose name is not a valid identifier in the script
language cannot be referenced this way.

## Parameters

Names are given in snake_case; the camelCase spelling of each (`file_encoding` /
`fileEncoding`) works too.

| Parameter | Description | Default |
| --- | --- | --- |
| `files` | Comma-separated file paths. | |
| `directories` | Comma-separated directory paths whose files are crawled. | |
| `recursive` | Whether `directories` are scanned below their top level. | `false` |
| `max_depth` | How far below each directory the scan may descend when `recursive=true`. | `10` |
| `include_pattern` | Regular expression that a file's absolute path must match in full. | |
| `exclude_pattern` | Regular expression that a file's absolute path must not match in full. | |
| `file_suffixes` | Comma-separated file suffixes to accept, matched case-insensitively. | `.json,.jsonl` |
| `file_encoding` | Character encoding of the files. | `UTF-8` |
| `format` | Document shape: `auto`, `jsonl` or `json`. | `auto` |
| `root_path` | JSON Pointer selecting the part of each document to read records from, e.g. `/data/items`. | |

`files`, `directories` or both must be set; they are not exclusive, and each file
is read once however many of them reach it. Files named in `files` are crawled in
the given order, and the files found under a directory are crawled oldest first.

An unusable `format`, `include_pattern`, `exclude_pattern` or `urls` value ends the
crawl before anything is read, and is recorded as a failure named after that
parameter. An unusable `max_depth` is reported in the log and the default is used.

### `root_path`

A JSON Pointer to a nested array indexes its elements as records:

```
root_path=/data/items
```

```json
{ "meta": { "count": 2 }, "data": { "items": [ { "id": "1" }, { "id": "2" } ] } }
```

A pointer that lands on an object instead yields that object as a single record,
and a pointer that matches nothing yields no records. `root_path` outranks
`format`, because a document reached through a pointer is never read line by line;
setting both logs a warning saying so.

### `format`

`auto` reads the start of the document and decides from its grammar, which is
enough for a well-formed file of any of the three shapes. Set `format=jsonl`
explicitly for a JSON Lines file whose leading lines may be broken or very long -
a banner, a progress log, a record cut off mid-transfer - since those are what the
detection has to read past.

The setting also decides what a bad record costs. Each line of a JSON Lines file is
parsed on its own, so a malformed line costs that line only. The other shapes are
read as a token stream, where a failure can swallow the record that follows, and a
document truncated mid-object never recovers: the source is abandoned with a
warning once it has produced only failures for long enough.

## Notes

Parameters whose names match `app.encrypt.property.pattern` (by default anything
ending in `password`, `key`, `token` or `secret`) are visible to scripts as `null`,
so a credential in the data store parameters cannot be copied into an indexed
field. A record field of the same name still takes priority, as any record field
does over a parameter.

## Documentation

See [JSON Connector](https://fess.codelibs.org/15.8/config/datastore/ds-json.html)
in the Fess documentation.
