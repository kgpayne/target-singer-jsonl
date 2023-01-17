# `target-singer-jsonl`

This is a [Singer](https://singer.io) target that receives Singer messages following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) and writes them stream-wise to JSONL formatted and g-zipped files.
File writing is done via the `smart_open` python package, supporting local disk as well as many other destinations (e.g. S3).

## Installation

```shell
pip install target-singer-jsonl
```

## Settings

```python
{
    "destination": "local",
    "add_record_metadata": False,
    "local": {
        "folder": ".secrets/output/"
    },
    "s3": {
        "bucket": "my-s3-bucket",
        "prefix": "put/files/in/here/"
    },
}
```

| Setting             | Required | Default | Description                                                                     |
| :------------------ | :------: | :-----: | :------------------------------------------------------------------------------ |
| destination         |  False   | `local` | The destination configuration to use. Currently `local` and `s3` are supported. |
| add_record_metadata |  False   | `true`  | Whether to inject `_sdc_*` metadata columns.                                    |
| local.folder        |  False   |         | Output folder to write `.singer.gz` stream files to.                            |
| s3.bucket           |  False   |         | S3 bucket.                                                                      |
| s3.prefix           |  False   |         | S3 key prefix to write `.singer.gz` stream files under.                         |

**Note:** `target-singer-jsonl` will automatically create a folder/key per stream (using the stream name) as well as using the stream name and a timestamp for each file.
Therefore the complete scheme is:

```
# local
<folder>/<stream name>/<stream name>-<timestamp>.singer.gz
# s3
s3://<bucket>/<prefix>/<stream name>/<stream name>-<timestamp>.singer.gz
```

Timestamps are of the form `%Y%m%dT%H%M%S%z`, and if folders/keys do not exist, they will be created.

## Usage

```bash
# pipe stream input from a single file
cat output/records.jsonl | target-singer-jsonl --config example_config.json
# pipe target input from a Singer tap
tap-carbon-intensity | target-singer-jsonl --config example_config.json
```

## Limitations

`target-singer-jsonl` is in **development** so may not work 100% as expected.
It was also not built using the [Meltano Singer SDK](https://sdk.meltano.com/en/latest/), due to its low-level handling of Singer messages.
This may change in future, however it currently means that some advanced features are not yet available.

## Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano run job-simple-test
```
