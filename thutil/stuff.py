from typing import Generator


def chunk_list(input_list: list, n: int) -> Generator:
    """Yield successive n-sized chunks from `input_list`."""
    for i in range(0, len(input_list), n):
        yield input_list[i : i + n]


### ANCHOR: string modifier
def fill_text_center(input_text="example", fill="-", max_length=60):
    """Create a line with centered text."""
    text = f" {input_text} "
    return text.center(max_length, fill)


def fill_text_left(input_text="example", left_margin=15, fill="-", max_length=60):
    """Create a line with left-aligned text."""
    text = f"{(fill * left_margin)} {input_text} "
    return text.ljust(max_length, fill)


def fill_text_box(input_text="", fill=" ", sp="|", max_length=60):
    """Put the string at the center of |  |."""
    strs = input_text.center(max_length, fill)
    box_text = sp + strs[1 : len(strs) - 1 :] + sp
    return box_text


def color_text(text: str, color: str = "red") -> str:
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
