import base64
import random
import time
from typing import Generator


def chunk_list(input_list: list, n: int) -> Generator:
    """Yield successive n-sized chunks from `input_list`."""
    for i in range(0, len(input_list), n):
        yield input_list[i : i + n]


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


def uui_time() -> str:
    timestamp = int(time.time() * 1000)
    rand = random.getrandbits(10)
    unique_value = (timestamp << 10) | rand  # Combine timestamp + random bits
    text = base64.urlsafe_b64encode(unique_value.to_bytes(8, "big")).decode().rstrip("=")
    return text.replace("-", "_")
