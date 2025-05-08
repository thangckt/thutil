import asyncio
import datetime
import logging
import os
import time
import warnings

# from alff.util.key import FILE_LOG_DISPATCH
from copy import deepcopy
from math import ceil
from pathlib import Path

from dpdispatcher import Machine, Resources, Submission, Task
from dpdispatcher.dlog import dlog
from thutil.stuff import chunk_list, text_color

##### SECTION: Dispatching jobs


##### ANCHOR: Synchronous submission
def prepare_submission(
    mdict_machine: dict,
    mdict_resources: dict,
    command_list: list[str],
    work_dir: str,
    task_dirs: list[str],
    forward_files: list[str],
    backward_files: list[str],
    forward_common_files: list[str],
    outlog: str,
    errlog: str,
):
    """Function to prepare the submission object for dispatching jobs."""
    ### revise input path to absolute path and as_string
    abs_mdict_machine = mdict_machine.copy()
    abs_mdict_machine["local_root"] = Path("./").resolve().as_posix()

    machine = Machine.load_from_dict(abs_mdict_machine)
    resources = Resources.load_from_dict(mdict_resources)

    command = " &&\n".join(command_list)

    ### Define the task_list
    task_list = [None] * len(task_dirs)
    for i, path in enumerate(task_dirs):
        task_list[i] = Task(
            command=command,
            task_work_path=path,
            forward_files=forward_files,
            backward_files=backward_files,
            outlog=outlog,
            errlog=errlog,
        )

    ### make submission
    submission = Submission(
        work_base=work_dir,
        machine=machine,
        resources=resources,
        task_list=task_list,
        forward_common_files=forward_common_files,
        backward_common_files=[],
    )
    return submission


def submit_job_chunk(
    mdict_machine: dict,
    mdict_resources: dict,
    command_list: list[str],
    work_dir: str,
    task_dirs: list[str],
    forward_files: list[str],
    backward_files: list[str],
    forward_common_files: list[str],
    outlog: str,
    errlog: str,
    job_limit: int,
    machine_index: int = 0,
    Logger: object = None,
):
    """Function to submit a job to the cluster:
    - Prepare the task list
    - Make the submission and wait for the job to finish
    - Download the results

    Note:
    - Improved version of `submit_job` to split the task_dirs into chunks and submit them.
    - When when using `Local` context, should use other account for submission jobs. This to avoid interference with the current shell environment.
    """
    text = text_color(
        f"Assigned {len(task_dirs)} jobs to Machine {machine_index} \n{remote_info(mdict_machine)}",
        color=COLOR_MAP[machine_index],
    )
    Logger.info(text)

    chunks = chunk_list(task_dirs, job_limit)
    old_time = None
    for chunk_index, task_dirs_1chunk in enumerate(chunks):
        new_time = time.time()
        text = info_current_dispatch(
            task_dirs,
            job_limit,
            chunk_index,
            task_dirs_1chunk,
            old_time,
            new_time,
        )
        Logger.info(text)
        submission = prepare_submission(
            mdict_machine=mdict_machine,
            mdict_resources=mdict_resources,
            command_list=command_list,
            work_dir=work_dir,
            task_dirs=task_dirs_1chunk,  # this key is different from the original submit_job
            forward_files=forward_files,
            backward_files=backward_files,
            forward_common_files=forward_common_files,
            outlog=outlog,
            errlog=errlog,
        )
        submission.run_submission()
        old_time = new_time
    return


##### ANCHOR: Asynchronous submission
async def async_submit_job_chunk(
    mdict_machine: dict,
    mdict_resources: dict,
    command_list: list[str],
    work_dir: str,
    task_dirs: list[str],
    forward_files: list[str],
    backward_files: list[str],
    forward_common_files: list[str],
    outlog: str,
    errlog: str,
    job_limit: int,
    machine_index: int = 0,
    Logger: object = None,
):
    """Convert `submit_job_chunk()` into an async function but only need to wait for the completion of the entire `for` loop (without worrying about the specifics of each operation inside the loop)
    NOTE:
        - An async function normally contain a `await ...` statement to be awaited (yield control to event loop)
        - If the 'event loop is blocked' by a asynchronous function (it will not yield control to event loop), the async function will wait for the completion of the synchronous function. So, the async function will not be executed asynchronously. Try to use `await asyncio.to_thread()` to run the synchronous function in a separate thread, so that the event loop is not blocked.
    """
    text = text_color(
        f"Assigned {len(task_dirs)} jobs to Machine {machine_index} \n{remote_info(mdict_machine)}",
        color=COLOR_MAP[machine_index],
    )
    Logger.info(text)

    chunks = chunk_list(task_dirs, job_limit)
    timer = {f"oldtime_{machine_index}": None}  # dynamic variable name
    for chunk_index, task_dirs_1chunk in enumerate(chunks):
        timer[f"newtime_{machine_index}"] = time.time()
        text = info_current_dispatch(
            task_dirs,
            job_limit,
            chunk_index,
            task_dirs_1chunk,
            timer[f"oldtime_{machine_index}"],
            timer[f"newtime_{machine_index}"],
            machine_index,
        )
        Logger.info(text)
        submission = prepare_submission(
            mdict_machine=mdict_machine,
            mdict_resources=mdict_resources,
            command_list=command_list,
            work_dir=work_dir,
            task_dirs=task_dirs_1chunk,  # this key is different from the original submit_job
            forward_files=forward_files,
            backward_files=backward_files,
            forward_common_files=forward_common_files,
            outlog=outlog,
            errlog=errlog,
        )
        # await asyncio.to_thread(submission.run_submission, check_interval=30)  # this is old, may cause (10054) error
        await run_submission_wrapper(submission, check_interval=30, machine_index=machine_index)
        timer[f"oldtime_{machine_index}"] = timer[f"newtime_{machine_index}"]
    return


machine_locks = {}  # Dictionary to store per-machine locks


async def run_submission_wrapper(submission, check_interval=30, machine_index=0):
    """Ensure only one instance of 'submission.run_submission' runs at a time.
    - If use one global lock for all machines, it will prevent concurrent execution of submissions on different machines. Therefore, each machine must has its own lock, so different machines can process jobs in parallel.
    """
    lock = _get_machine_lock(machine_index)  # Get per-machine lock
    async with lock:  # Prevents concurrent execution
        try:
            await asyncio.to_thread(submission.run_submission, check_interval=check_interval)
        except Exception as e:
            print(f"Error in dispatcher submission: {e}")
        finally:
            del submission  # free up memory
    return


def _get_machine_lock(machine_index):
    if machine_index not in machine_locks:
        machine_locks[machine_index] = asyncio.Lock()
    return machine_locks[machine_index]


##### ANCHOR: Submit to multiple machines
async def submit_job_multi_remotes(
    mdict: dict,
    work_dir: str,
    task_dirs: list[str],
    prepare_command_list: callable,
    forward_files: list[str],
    backward_files: list[str],
    forward_common_files: list[str],
    job_prefix: str = "dft",
    Logger: object = None,
):
    """Submit jobs to multiple machines asynchronously.
    Args:
        mdict (dict): the dict contains multiple machines information.
        prepare_command_list(callable): function to prepare the command list for each machine.
        job_prefix(str): the type of job to run on remote machines. Choices: 'dft', 'md', 'train'.
    """
    remote_machine_list = [v for k, v in mdict.items() if k.startswith(job_prefix)]
    num_machines = len(remote_machine_list)
    Logger.info(f"Distribute {len(task_dirs)} jobs across {num_machines} remote machines")

    remain_task_dirs = deepcopy(task_dirs)
    background_runs = []
    for i, remote in enumerate(remote_machine_list):
        mdict_machine = remote["machine"]
        mdict_resources = remote["resources"]
        job_limit = remote.get("job_limit", 5)
        current_work_load = remote.get("work_load_ratio", None)

        ### Divide task_dirs
        if not current_work_load:
            current_work_load = 1.0 / num_machines
            num_jobs = ceil(len(remain_task_dirs) * current_work_load)
        else:
            num_jobs = ceil(len(task_dirs) * current_work_load)

        if num_jobs <= len(remain_task_dirs):
            current_task_dirs = remain_task_dirs[:num_jobs]
            remain_task_dirs = remain_task_dirs[num_jobs:]
            num_machines -= 1
        else:
            current_task_dirs = remain_task_dirs
            remain_task_dirs = []

        ### Prepare command_list
        command_list = prepare_command_list(remote)

        ### Submit jobs
        if len(current_task_dirs) > 0:
            async_task = async_submit_job_chunk(
                mdict_machine=mdict_machine,
                mdict_resources=mdict_resources,
                command_list=command_list,
                work_dir=work_dir,
                task_dirs=current_task_dirs,
                forward_files=forward_files,
                backward_files=backward_files,
                forward_common_files=forward_common_files,
                outlog=f"{job_prefix}.log",
                errlog=f"{job_prefix}.err",
                job_limit=job_limit,
                machine_index=i,
                Logger=Logger,
            )
            background_runs.append(async_task)
            # Logger.debug(f"Assigned coroutine to Machine {i}: {async_task}")
    await asyncio.gather(*background_runs)
    return


##### !SECTION


##### ANCHOR: helper functions
COLOR_MAP = {0: "blue", 1: "green", 2: "cyan", 3: "yellow", 4: "red", 5: "purple"}


def info_current_dispatch(
    task_dirs,
    job_limit,
    chunk_index,  # start from 0
    task_dirs_1chunk,
    old_time=None,
    new_time=None,
    machine_index=0,
) -> str:
    """Return the information of the current chunk of tasks."""
    total_chunks = ceil(len(task_dirs) / job_limit)
    remaining_tasks = len(task_dirs) - chunk_index * job_limit
    text = f"Machine {machine_index}, is running {len(task_dirs_1chunk)} of {remaining_tasks} jobs (chunk {chunk_index + 1}/{total_chunks})."
    ### estimate time remaining
    if old_time is not None and new_time is not None:
        time_elapsed = new_time - old_time
        time_remaining = time_elapsed * (total_chunks - chunk_index)
        duration = str(datetime.timedelta(seconds=time_remaining))
        hm_format = ":".join(str(duration).split(":")[:-1])
        text += f" Time left {hm_format}"
    text = text_color(text, color=COLOR_MAP[machine_index])  # make color
    return text


def remote_info(mdict) -> str:
    """Return the remote machine information.
    Args:
        mdict (dict): the machine dictionary
    """
    remote_path = mdict["remote_root"]
    hostname = mdict["remote_profile"]["hostname"]
    info_text = f"{' ' * 11}Remote host: {hostname}\n"
    info_text += f"{' ' * 11}Remote path: {remote_path}"
    return info_text


def preserve_env_var(exclude_prefixs: list[str] = []):
    """Preserve the environment variables that are not in the exclude list."""
    env_vars = os.environ.copy()
    select_keys = [
        k for k in env_vars.keys() if not any(k.startswith(prefix) for prefix in exclude_prefixs)
    ]
    select_vars = {k: env_vars[k] for k in select_keys}
    return select_vars


##### ANCHOR: Change the path of logfile
def change_logpath_dispatcher(newlogfile: str):
    """Change the path of the logfile for dpdispatcher."""
    try:
        for hl in dlog.handlers[:]:  # Remove all old handlers
            hl.close()
            dlog.removeHandler(hl)

        fh = logging.FileHandler(newlogfile)
        fmt = logging.Formatter(
            "%(asctime)s | %(name)s-%(levelname)s: %(message)s", "%Y%b%d %H:%M:%S"
        )
        fh.setFormatter(fmt)
        dlog.addHandler(fh)
        dlog.info(f"LOG INIT: dpdispatcher log direct to {newlogfile}")

        ### Remove the old log file if it exists
        if Path("./dpdispatcher.log").is_file():
            Path("./dpdispatcher.log").unlink()
    except Exception as e:
        warnings.warn(f"Changing DPDISPATCHER logfile_path met an error {e}. Use the default path.")
    return


# change_logpath_dispatcher(FILE_LOG_DISPATCH)
