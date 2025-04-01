import base64
import random
import string
import time
from typing import Generator


def chunk_list(input_list: list, n: int) -> Generator:
    """Yield successive n-sized chunks from `input_list`."""
    for i in range(0, len(input_list), n):
        yield input_list[i : i + n]


### ANCHOR: index tools
def unpack_indices(list_inputs: list[int | str]) -> list[int]:
    """Expand the input list of indices to a list of integers.
    Eg: list_inputs = [1, 2, "3-5:2", "6-10"]
    """
    idx = []
    for item in list_inputs:
        if isinstance(item, int):
            idx.append(item)
        elif isinstance(item, str):
            parts = item.split(":")
            step = int(parts[1]) if len(parts) > 1 else 1
            start, end = map(int, parts[0].split("-"))
            idx.extend(range(start, end + 1, step))
    return idx


### ANCHOR: string modifier
def text_fill_center(input_text="example", fill="-", max_length=60):
    """Create a line with centered text."""
    text = f" {input_text} "
    return text.center(max_length, fill)


def text_fill_left(input_text="example", left_margin=15, fill="-", max_length=60):
    """Create a line with left-aligned text."""
    text = f"{(fill * left_margin)} {input_text} "
    return text.ljust(max_length, fill)


def text_fill_box(input_text="", fill=" ", sp="|", max_length=60):
    """Put the string at the center of |  |."""
    strs = input_text.center(max_length, fill)
    box_text = sp + strs[1 : len(strs) - 1 :] + sp
    return box_text


def text_color(text: str, color: str = "blue") -> str:
    """ANSI escape codes for color the text."""
    color_dict = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "blue": "\033[94m",
        "purple": "\033[95m",
        "cyan": "\033[96m",
        "white": "\033[97m",
        "bold": "\033[1m",
        "underline": "\033[4m",
        "nocolor": "\033[0m",  # reset color
    }
    text = color_dict[color] + text + color_dict["nocolor"]
    return text


def time_uuid() -> str:
    timestamp = int(time.time() * 1.0e6)
    rand = random.getrandbits(10)
    unique_value = (timestamp << 10) | rand  # Combine timestamp + random bits
    text = base64.urlsafe_b64encode(unique_value.to_bytes(8, "big")).decode().rstrip("=")
    return text.replace("-", "_")


def simple_uuid():
    """Generate a simple random UUID of 4 digits."""
    rnd_letter = random.choice(string.ascii_uppercase)  # ascii_letters
    rnd_num = random.randint(100, 999)
    return f"{rnd_letter}{rnd_num}"
