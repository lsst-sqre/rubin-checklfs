from pathlib import Path


def str_bool(inp: str) -> bool:
    inp = inp.upper()
    if not inp or inp == "0" or inp.startswith("F") or inp.startswith("N"):
        return False
    return True


def path(str_path: str) -> Path:
    # Gets us syntactic validation for free, except that there's not much
    # that would be an illegal path other than 0x00 as a character in it.
    return Path(str_path)
