# target-singer-jsonl

This is a [Singer](https://singer.io) target that reads JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) and writes it to JSONL formatted files.
File writing is done via the `smart_open` python package, supporting local disk as well as many other destinations (e.g. S3).
