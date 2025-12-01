"""
Formatter for CLI responses.

In the interest of making our CLI MCP friendly as well as VSCode plugin friendly, we need more structured responses to avoid parsing the output manually.

For this purpose we add a new kind of exception that can be raised to prompt the user for a confirmation.
Any exception is caught by the jsonify_exceptions_if_json_format decorator and turned into a JSON response.
"""

import functools
import json
import sys
import traceback
from enum import Enum


class PromptException(Exception):
    """
    Exception to raise when user prompt is needed.
    Args:
        prompt: The prompt to display to the user.
        options: The options to display to the user.
        instructions: The instructions for implementation.
    """
    def __init__(self, prompt, options, instructions):
        super().__init__(prompt)
        self.prompt = prompt
        self.options = options
        self.instructions = instructions

class Format(Enum):
    JSON = "json"
    TEXT = "text"

def format_print(lines, format: Format = Format.TEXT):
    """ Format aware print. """
    if format != Format.JSON:
        print(lines)


def jsonify_exceptions_if_json_format(func):
    """ Turn exceptions into JSON if the format is JSON. If a prompt exception is raised, return the prompt and options."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        use_json = kwargs.get("format") == Format.JSON
        try:
            return func(*args, **kwargs)
        except PromptException as e:
            if use_json:
                print(json.dumps({
                    "prompt": e.prompt,
                    "options": e.options,
                    "instructions": e.instructions,
                }))
                sys.exit(0)
        except Exception as e:
            if use_json:
                print(json.dumps({
                    "error": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                }))
                sys.exit(1)
            raise e
    return wrapper
