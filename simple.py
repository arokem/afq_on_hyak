
import os.path as op
from tempfile import mkdtemp

import pydra


scratch_dir = "/gscratch/escience/arokem/"
scratch_dir_tmp = op.join(scratch_dir, "tmp_")
cache_dir_tmp = mkdtemp(prefix=scratch_dir_tmp)


@pydra.mark.task
def task(subject):
    print("I am doing something really simple")


subject_list = ["01", "02"]
t = task(subject=subject_list, cache_dir=cache_dir_tmp).split("subject")

with pydra.Submitter(plugin="slurm",
                     sbatch_args=f"-J task -p gpu-a40 -A escience --mem=58G --time=10:00:00 -o /gscratch/escience/arokem/logs/task.out -e /gscratch/escience/arokem/logs/task.err --mail-user=arokem@uw.edu --mail-type=ALL") as sub:
    sub(runnable=t)
