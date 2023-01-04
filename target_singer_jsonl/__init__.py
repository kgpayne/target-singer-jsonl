#!/usr/bin/env python3

import argparse
import io
import json
import logging
import sys
from datetime import datetime

from jsonschema.validators import Draft4Validator

logger = logging.getLogger(__name__)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug(f"Emitting state {line}")
        sys.stdout.write(f"{line}\n")
        sys.stdout.flush()


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    now = datetime.now().strftime("%Y%m%dT%H%M%S")

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

        if t == "RECORD":
            if "stream" not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")
            if o["stream"] not in schemas:
                raise Exception(
                    f'A record for stream {o["stream"]} was encountered before a corresponding schema'
                )

            # Get schema for this record's stream
            schema = schemas[o["stream"]]

            # Validate record
            validators[o["stream"]].validate(o["record"])

            # If the record needs to be flattened, uncomment this line
            # flattened_record = flatten(o['record'])

            # TODO: Process Record message here..

            state = None
        elif t == "STATE":
            logger.debug(f'Setting state to {o["value"]}')
            state = o["value"]
        elif t == "SCHEMA":
            if "stream" not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")
            stream = o["stream"]
            schemas[stream] = o["schema"]
            validators[stream] = Draft4Validator(o["schema"])
            if "key_properties" not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o["key_properties"]
        else:
            raise Exception(f"Unknown message type {t} in message {o}")
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
