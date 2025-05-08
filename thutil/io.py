from pathlib import Path


### ANCHOR: Load data from file
def combine_text_files(files: list[str], output_file: str, chunk_size: int = 1024):
    """
    Combine text files into a single file in a memory-efficient. Read and write in chunks to avoid loading large files into memory

    Args:
        files (list[str]): List of file paths to combine.
        output_file (str): Path to the output file.
        chunk_size (int, optional): Size of each chunk in KB to read/write. Defaults to 1024 KB.
    """
    chunk_size_byte = chunk_size * 1024
    ### Create parent folder if not exist
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    ### Open the output file for writing and append each file's content incrementally
    with open(output_file, "w") as outfile:
        for file in files:
            with open(file, "r") as infile:
                while chunk := infile.read(chunk_size_byte):  # Read in chunks
                    outfile.write(chunk)
    return


def download_rawtext(url: str, outfile: str = None) -> str:
    """Download raw text from a URL."""
    import requests

    res = requests.get(url)
    text = res.text
    if outfile is not None:
        with open(outfile, "w") as f:
            f.write(text)
    return text
