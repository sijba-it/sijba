#!/usr/bin/env python3
"""Echo handler: responds with the same pipeline_text. Useful for testing."""
import json
import sys

envelope = json.load(sys.stdin)
text = envelope.get("pipeline_text", "")

response = {
    "continue": False,
    "respond": {
        "text": f"echo: {text}",
        "target": "source",
    },
}

print(json.dumps(response))
