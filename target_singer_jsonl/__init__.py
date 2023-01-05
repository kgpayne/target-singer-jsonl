#!/usr/bin/env python3

import argparse
import io
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from jsonschema.validators import Draft4Validator
from smart_open.smart_open_lib import patch_pathlib

_ = patch_pathlib()  # replace `Path.open` with `smart_open.open`

logger = logging.getLogger(__name__)

example_config = {
    "destination": "local",
    "s3": {"bucket": "my-s3-bucket", "prefix": "put/files/in/here/"},
    "local": {"folder": ".secrets/output/"},
}

stream_files = {}
stream_lines = {}
now = datetime.now().strftime("%Y%m%dT%H%M%S%z")


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug(f"Emitting state {line}")
        sys.stdout.write(f"{line}\n")
        sys.stdout.flush()


def get_file_path(config, stream):
    destination = config.get("destination", "local")
    if destination == "local":
        return Path(config["local"]["folder"]).joinpath(
            f"{stream}/{stream}-{now}.jsonl.gz"
        )
    raise KeyError(f"Destination {destination} not supported.")


def write_lines(config, stream, lines):
    if stream not in stream_files:
        stream_files[stream] = get_file_path(config, stream)

    destination = config.get("destination", "local")
    if destination == "local":
        stream_files[stream].parent.mkdir(parents=True, exist_ok=True)

    with stream_files[stream].open("w") as outfile:
        for line in lines:
            outfile.write(line)


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error(f"Unable to parse:\n{line}")
            raise

        if "type" not in o:
            raise Exception(f"Line is missing required key 'type': {line}")
        t = o["type"]

        if t != "STATE":
            if "stream" not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")

            stream = o["stream"]

            if stream not in stream_lines:
                stream_lines[stream] = []

            # persisting STATE messages is problematic when splitting records into separate
            # files, therefore we omit them and allow tap-singer-jsonl to create new
            # state messages from observed records
            stream_lines[stream].append(line)

        if t == "RECORD":

            if stream not in schemas:
                raise Exception(
                    f"A record for stream {stream} was encountered before a corresponding schema"
                )

            # Get schema for this record's stream
            schema = schemas[stream]

            # Validate record
            validators[stream].validate(o["record"])

            state = None
        elif t == "STATE":
            logger.debug(f'Setting state to {o["value"]}')
            state = o["value"]
        elif t == "SCHEMA":
            schemas[stream] = o["schema"]
            validators[stream] = Draft4Validator(o["schema"])
            if "key_properties" not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o["key_properties"]
        else:
            raise Exception(f"Unknown message type {t} in message {o}")

    for stream, messages in stream_lines.items():
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
