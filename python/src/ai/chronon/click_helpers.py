import functools
import sys

import click
from click import style

from ai.chronon.repo.compile import __compile


def handle_compile(func):
    """ 
    Handler for compiling the confs before running commands
    Requires repo arg
    """
    @click.option("--skip-compile", help="Skip compile before running the command", is_flag=True)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not kwargs.get("skip_compile"):
            sys.path.append(kwargs.get("repo"))
            __compile(kwargs.get("repo"), force=kwargs.get("force"))
        return func(*args, **kwargs)
    return wrapper


def handle_conf_not_found(log_error=True, callback=None):
    """
    Handler for when a conf is not found
    """
    def wrapper(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as e:
                if log_error:
                    print(style(f"File not found in {func.__name__}: {e}", fg="red"))
                if callback:
                    callback(*args, **kwargs)
                return

        return wrapped

    return wrapper