#!/usr/bin/env python3

import argparse
import io
import json
import logging
import sys
import time
from datetime import datetime
from functools import reduce
from pathlib import Path

from jsonschema.validators import Draft4Validator
from smart_open import open
from smart_open.smart_open_lib import patch_pathlib

_ = patch_pathlib()  # replace `Path.open` with `smart_open.open`

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

logger = logging.getLogger(__name__)

example_config = {
    "destination": "local",
    "s3": {"bucket": "my-s3-bucket", "prefix": "put/files/in/here/"},
    "local": {"folder": ".secrets/output/"},
    "add_record_metadata": False,
}

stream_files = {}
stream_lines = {}
file_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%z")
target_start_timestamp = datetime.now().isoformat()


def join_slash(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


def urljoin(*args):
    return reduce(join_slash, args) if args else ""


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug(f"Emitting state {line}")
        sys.stdout.write(f"{line}\n")
        sys.stdout.flush()


def get_file_path(stream, destination, config):
    filename = f"{stream}/{stream}-{file_timestamp}.singer.gz"
    if destination == "local":
        return Path(config["folder"]).joinpath(filename)
    elif destination == "s3":
        bucket = config["bucket"]
        prefix = config["prefix"]
        return urljoin(f"s3://{bucket}/{prefix}/", filename)
    else:
        raise KeyError(f"Destination {destination} not supported.")


def write_lines_local(destination, config, stream, lines):
    if stream not in stream_files:
        stream_files[stream] = get_file_path(
            stream=stream, destination=destination, config=config
        )
    stream_files[stream].parent.mkdir(parents=True, exist_ok=True)

    with stream_files[stream].open("w", encoding="utf-8") as outfile:
        logging.info(f"Writing to file: {stream_files[stream]}")
        for line in lines:
            outfile.write(line + "\n")


def write_lines_s3(destination, config, stream, lines):
    if stream not in stream_files:
        stream_files[stream] = get_file_path(
            stream=stream, destination=destination, config=config
        )
    with open(stream_files[stream], "w", encoding="utf-8") as outfile:
        logging.info(f"Writing to file: {stream_files[stream]}")
        for line in lines:
            outfile.write(line + "\n")


def write_lines(config, stream, lines):
    destination = config.get("destination", "local")
    if destination == "local":
        return write_lines_local(
            destination=destination,
            config=config[destination],
            stream=stream,
            lines=lines,
        )
    elif destination == "s3":
        return write_lines_s3(
            destination=destination,
            config=config[destination],
            stream=stream,
            lines=lines,
        )


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    add_record_metadata = config.get("add_record_metadata", True)

    # Loop over lines from stdin
    for line in lines:
        try:
            message = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error(f"Unable to parse:\n{line}")
            raise

        if "type" not in message:
            raise Exception(f"Line is missing required key 'type': {line}")
        t = message["type"]

        if t != "STATE":
            if "stream" not in message:
                raise Exception(f"Line is missing required key 'stream': {line}")

            stream = message["stream"]

            if stream not in stream_lines:
                stream_lines[stream] = []

        if t == "RECORD":
            if stream not in schemas:
                raise Exception(
                    f"A record for stream {stream} was encountered before a corresponding schema"
                )

            record = message["record"]
            # Get schema for this record's stream
            schema = schemas[stream]
            # Validate record
            validators[stream].validate(record)
            # Process record
            if add_record_metadata:
                now = datetime.now().isoformat()
                record.update(
                    {
                        "_sdc_extracted_at": message.get(
                            "time_extracted", target_start_timestamp
                        ),
                        "_sdc_received_at": now,
                        "_sdc_batched_at": now,
                        "_sdc_deleted_at": record.get("_sdc_deleted_at"),
                        "_sdc_sequence": int(round(time.time() * 1000)),
                        "_sdc_table_version": message.get("version"),
                    }
                )
            # Queue message for write
            state = None
            stream_lines[stream].append(json.dumps(message))

        elif t == "SCHEMA":
            schemas[stream] = message["schema"]
            validators[stream] = Draft4Validator(message["schema"])
            if "key_properties" not in message:
                raise Exception("key_properties field is required")
            key_properties[stream] = message["key_properties"]
            # Add metadata properties
            if add_record_metadata:
                properties_dict = schemas[stream]["properties"]
                for col in {
                    "_sdc_extracted_at",
                    "_sdc_received_at",
                    "_sdc_batched_at",
                    "_sdc_deleted_at",
                }:
                    properties_dict[col] = {
                        "type": ["null", "string"],
                        "format": "date-time",
                    }
                for col in {"_sdc_sequence", "_sdc_table_version"}:
                    properties_dict[col] = {"type": ["null", "integer"]}
            # Queue message for write
            stream_lines[stream].append(json.dumps(message))

        elif t == "STATE":
            # persisting STATE messages is problematic when splitting records into separate
            # files, therefore we omit them and allow tap-singer-jsonl to create new
            # state messages from observed records on read
            logger.debug(f'Setting state to {message["value"]}')
            state = message["value"]

        else:
            raise Exception(f"Unknown message type {t} in message {message}")

    for stream, messages in stream_lines.items():
        if len[messages] > 1:  # don't write SCHEMA without any RECORDs
            write_lines(config, stream, messages)

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    state = persist_lines(config, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    main()
