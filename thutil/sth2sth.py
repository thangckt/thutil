from pathlib import Path
from typing import Union


def file2str(file_path: Union[str, Path]) -> str:
    with open(file_path, "r") as f:
        text = f.read()
    return text


def str2file(text: str, file_path: Union[str, Path]) -> None:
    with open(file_path, "w") as f:
        f.write(text)
    return


def file2list(file_path: Union[str, Path]) -> list[str]:
    with open(file_path, "r") as f:
        lines = f.readlines()
    lines = [line.rstrip() for line in lines]
    return lines


def list2file(text_list: list, file_path: Union[str, Path]) -> None:
    text = "\n".join(text_list)
    str2file(text, file_path)
    return


def float2str(floatnum, decimals=6):
    """convert float number to str
    REF: https://stackoverflow.com/questions/2440692/formatting-floats-without-trailing-zeros

    Args:
        floatnum (float): float number
        fmt (str): format of the output string

    Returns:
        s (str): string of the float number
    """
    s = f"{floatnum:.{decimals}f}".rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s
