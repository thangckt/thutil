import shutil
from glob import glob
from pathlib import Path


##### ANCHOR: Make dirs
def make_dir(path: str, backup: bool = True):
    """Create a directory with a backup option."""
    path = Path(path)
    ### Create a backup path with a counter suffix
    if path.is_dir() and backup:
        counter = 0
        while True:
            bk_path = path.with_name(f"{path.name}_bk{counter:03d}")
            if not bk_path.exists():
                shutil.move(str(path), str(bk_path))
                break
            counter += 1
    ### Create the new directory
    path.mkdir(parents=True, exist_ok=True)
    return


def make_dir_ask_backup(dir_path: str):
    """Make a directory and ask for backup if the directory already exists."""
    if Path(dir_path).is_dir():
        ans = ask_yes_no(
            f"""The directory "{dir_path}" already existed. Select an action: [yes/no/backup]?\n  * Yes: overwrite the existing directory and continue/update unfinished tasks.\n  * No: interrupt and exit process.\n  * Backup: backup the existing directory and perform fresh tasks."""
        )
        if ans == "yes":
            print("\tOverwrite the existing directory")
            make_dir(dir_path, backup=False)
        elif ans == "backup":
            print("\tBackup the existing directory")
            make_dir(dir_path, backup=True)
        if ans == "no":
            print("\tSkip the building process due to existing directory")
            return
    else:
        make_dir(dir_path, backup=False)
    return


def ask_yes_no(question: str) -> str:
    """Asks a yes/no/backup question and returns the response."""
    while True:
        answer = input(f"{question} \n\tPlease input your choice (y/n/b): ").strip().lower()
        if answer in ["yes", "y"]:
            return "yes"
        elif answer in ["no", "n"]:
            return "no"
        elif answer in ["backup", "b"]:
            return "backup"
        else:
            print("Please answer with 'yes', 'no', or 'backup'.")


##### ANCHOR: Collect files
def list_paths(paths: list[str], patterns: list[str], recursive=True) -> list[str]:
    """List all files/folders in given directories and their subdirectories that match the given patterns.

    Parameters
    ----------
    paths : list[str]
        The list of paths to search files/folders.
    patterns : list[str]
        The list of patterns to apply to the files. Each filter can be a file extension or a pattern.

    Returns:
    -------
    List[str]: A list of matching paths.

    Example:
    --------
    ```python
    folders = ["path1", "path2", "path3"]
    patterns = ["*.ext1", "*.ext2", "something*.ext3", "*folder/"]
    files = list_files_in_dirs(folders, patterns)
    ```

    Note:
    -----
    - glob() does not list hidden files by default. To include hidden files, use glob(".*", recursive=True).
    - When use recursive=True, must include `**` in the pattern to search subdirectories.
        - glob("*", recursive=True) will search all FILES & FOLDERS in the CURRENT directory.
        - glob("*/", recursive=True) will search all FOLDERS in the current CURRENT directory.
        - glob("**", recursive=True) will search all FILES & FOLDERS in the CURRENT & SUB subdirectories.
        - glob("**/", recursive=True) will search all FOLDERS in the current CURRENT & SUB subdirectories.
        - "**/*" is equivalent to "**".
        - "**/*/" is equivalent to "**/".
    - IMPORTANT: "**/**" will replicate the behavior of "**", then give unexpected results.

    """
    if not isinstance(paths, list):
        paths = [paths]

    result_paths = []
    for path in paths:
        for pattern in patterns:
            if recursive:
                result_paths.extend(glob(f"{path}/**/{pattern}", recursive=True))
            else:
                result_paths.extend(glob(f"{path}/**/{pattern}"))

    result_paths = list(set(result_paths))  # Remove duplicates
    paths = [Path(p).as_posix() for p in result_paths]
    return paths


def collect_files(paths: list[str], patterns: list[str]) -> list[str]:
    """Collect files from a list of paths (files/folders). Will search files in folders and their subdirectories.

    Parameters
    ----------
    paths : list[str]
        The list of paths to collect files from.
    patterns : list[str]
        The list of patterns to apply to the files. Each filter can be a file extension or a pattern.

    Returns:
    -------
    List[str]: A list of paths matching files.
    """
    if not isinstance(paths, list):
        paths = [paths]

    ### Detemine paths: dirs, files, and patterns
    files = [p for p in paths if Path(p).is_file()]
    dir_paths = [p for p in paths if Path(p).is_dir()]
    pattern_paths = [p for p in paths if "*" in p]

    ### Files from dir_paths
    search_files = list_paths(dir_paths, patterns, recursive=True)
    files.extend(search_files)

    ### Files from pattern_paths
    search_files = []
    for pattern_path in pattern_paths:
        search_files.extend(glob(pattern_path))

    files.extend(search_files)
    files = list(set(files))  # Remove duplicates
    files = [Path(p).as_posix() for p in files]
    return files


##### ANCHOR: change path names
def change_pathname(paths: list[str], old_string: str, new_string: str, replace: bool = False) -> None:
    """change path names

    Args:
        paths (list[str]): paths to the files/dirs
        old_string (str): old string in path name
        new_string (str): new string in path name
        replace (bool, optional): replace the old path name if the new one exists. Defaults to False.
    """
    ### Classify dirs and files
    paths = [p for p in paths if old_string in p]
    files = [p for p in paths if Path(p).is_file()]
    dirs = [p for p in paths if Path(p).is_dir()]
    print(f"There are {len(files)} files and {len(dirs)} directories, need to change.")

    ### Change the path names
    changed_count = 0
    for f in files:
        new_name = f.replace(old_string, new_string)
        if Path(new_name).exists() and not replace:
            print(f'WARNING: "{new_name}" already exists, skipping it.')
        else:
            Path(f).rename(new_name)
            changed_count += 1

    for d in dirs:
        new_name = d.replace(old_string, new_string)
        if Path(new_name).exists() and not replace:
            print(f'WARNING: "{new_name}" already exists, skipping it.')
        else:
            Path(d).rename(new_name)
            changed_count += 1

    print(f"Changed {changed_count} of {len(paths)} paths.")
    return


def remove_files(files: list[str]) -> None:
    """Remove files from a given list of file paths.

    Args:
        files (list[str]): list of file paths
    """
    _ = [Path(f).unlink() for f in files]
    return


def remove_dirs(dirs: list[str]) -> None:
    """Remove a list of directories.

    Parameters:
        dirs (list[str]): list of directories to remove.
    """
    _ = [shutil.rmtree(d) for d in dirs]
    return


def remove_files_in_paths(files: list, paths: list) -> None:
    """Remove files in the `files` list in the `paths` list."""
    _ = [Path(f"{p}/{f}").unlink() for p in paths for f in files if Path(f"{p}/{f}").exists()]
    return


def remove_dirs_in_paths(dirs: list, paths: list) -> None:
    """Remove directories in the `dirs` list in the `paths` list."""
    _ = [shutil.rmtree(f"{p}/{d}") for p in paths for d in dirs if Path(f"{p}/{d}").exists()]
    return


def copy_file(src_path: str, dest_path: str):
    """
    Copy a file/folder from the source path to the destination path.
    """
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
    new_path = shutil.copy2(src_path, dest_path)
    return new_path


def move_file(src_path: str, dest_path: str):
    """
    Move a file/folder from the source path to the destination path.
    """
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
    new_path = shutil.move(src_path, dest_path)
    return new_path


def scan_dirs(
    dirs: list[str],
    with_files: list[str],
    without_files: list[str] = [],
) -> list[str]:
    """Check if the folders contains and not contains some files.

    Args:
        dirs (list[str]): The paths of dirs to scan.
        with_files (list[str]): The files that should exist in the path.
        without_files (list[str], optional): The files that should not exist in the work_dir. Defaults to [].

    Returns:
        list[str]: The paths that meet the conditions.
    """
    found_paths = [p for p in dirs if all(Path(f"{p}/{f}").exists() for f in with_files)]
    found_paths = [p for p in found_paths if all(not Path(f"{p}/{f}").exists() for f in without_files)]
    return found_paths
