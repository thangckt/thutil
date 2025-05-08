import logging
import subprocess
from pathlib import Path

from thutil.stuff import text_fill_center, text_fill_left  # noqa: F401


def create_logger(
    logger_name: str = None,
    log_file: str = None,
    level: str = "INFO",
    level_logfile: str = None,
    format_: str = "info",
) -> logging.Logger:
    """Create and configure a logger with console and optional file handlers."""

    # Define logging levels and formats
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    # format_map = {
    #     "debug": "%(name)s-%(levelname)s: %(message)s | %(funcName)s:%(lineno)d",
    #     "info": "%(name)s-%(levelname)s: %(message)s",
    #     "file": "%(asctime)s | %(name)s-%(levelname)s: %(message)s",
    # }
    format_map = {
        "debug": "%(levelname)s: %(message)s | %(funcName)s:%(lineno)d",
        "info": "%(levelname)s: %(message)s",
        "file": "%(asctime)s | %(levelname)s: %(message)s",
    }

    # Set console and file logging levels
    c_level = level_map.get(level, logging.INFO)
    f_level = level_map.get(level_logfile, c_level)

    # Set logging formats
    format_console = format_map.get(format_, format_map["info"])
    format_file = format_map["file"]

    # Determine the logger name
    if not logger_name:
        logger_name = __name__

    # Create and configure the logger
    Logger = logging.getLogger(logger_name)
    Logger.setLevel(c_level)  # Ensures the logger can handle the specified log level

    # Create console handler
    c_handler = logging.StreamHandler()
    c_handler.setLevel(c_level)
    c_handler.setFormatter(logging.Formatter(format_console))
    Logger.addHandler(c_handler)

    # Create file handler if log_file is specified
    if log_file:
        f_handler = logging.FileHandler(log_file, mode="a")
        f_handler.setLevel(f_level)
        f_handler.setFormatter(logging.Formatter(format_file, "%Y%b%d %H:%M:%S"))
        Logger.addHandler(f_handler)

    return Logger


def check_package(
    package_name: str,
    auto_install: bool = False,
    git_repo: str = None,
    conda_channel: str = None,
):
    """Check if the required packages are installed"""
    try:
        __import__(package_name)
    except ImportError:
        if auto_install:
            _install_package(package_name, git_repo, conda_channel)
        else:
            raise ImportError(
                f"Required package `{package_name}` is not installed. Please install it.",
            )
    return


def _install_package(
    package_name: str,
    git_repo: str = None,
    conda_channel: str = None,
):
    """Install the required package:
        - Default using: `pip install -U {package_name}`
        - If `git_repo` is provided: `pip install -U git+{git_repo}`
        - If `conda_channel` is provided: `conda install -c {conda_channel} {package_name}`

    Args:
    ----
        package_name (str): package name
        git_repo (str): git path for the package. Default: None. E.g., http://somthing.git
        conda_channel (str): conda channel for the package. Default: None. E.g., conda-forge
    """
    try:
        print(f"Installing package: `{package_name}` ...")
        command = f"pip install -U {package_name}"
        if git_repo:
            command = f"pip install -U git+{git_repo}"
        else:
            command = f"conda install -c {conda_channel} {package_name}"
        subprocess.run(command, check=True)
        print("Install successfully!")
    except subprocess.CalledProcessError as e:
        raise RuntimeError((f"Error while installing the package: {e}"))
    return


def get_func_args(func):
    """Get the arguments of a function"""
    import inspect

    argspec = inspect.getfullargspec(func)
    no_default_args = ["no_default_value"] * (len(argspec.args) - len(argspec.defaults))
    all_values = no_default_args + list(argspec.defaults)
    argsdict = dict(zip(argspec.args, all_values))
    return argsdict


def dependency_info(
    modules=[
        "numpy",
        "polars",
        "thutil",
        "ase",
    ],
) -> str:
    """Get the dependency information"""
    text = "{}\n".format(text_fill_center("Dependencies", max_length=70))
    for pkg in modules:
        try:
            mm = __import__(pkg)
            ver = mm.__version__.split("+")[0]
            path = mm.__path__[0]
            text += "{:>12}  {:<12} {}\n".format(pkg, ver, Path(path).as_posix())
        except ImportError:
            text += "{:>12}  {:<12} {}\n".format(pkg, "unknown", "")
        except AttributeError:
            text += "{:>12}  {:<12} {}\n".format(pkg, "", "unknown version or path")
    ### Python version
    mm = __import__("sys")
    text += "{:>12}  {:<12} {}\n".format(
        "python",
        "{}.{}.{}".format(mm.version_info.major, mm.version_info.minor, mm.version_info.micro),
        Path(mm.executable).as_posix(),
    )
    return text
