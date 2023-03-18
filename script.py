
import os
import os.path as op
import s3fs
from pathlib import Path
from tempfile import mkdtemp

import pydra

from AFQ.api.group import GroupAFQ
from AFQ.definitions.image import ImageFile, ScalarImage
from AFQ.definitions.mapping import ItkMap
from AFQ.data.fetch import to_bids_description


scratch_dir = "/gscratch/escience/arokem/"
scratch_dir_tmp = op.join(scratch_dir, "tmp_")
cache_dir_tmp = mkdtemp(prefix=scratch_dir_tmp)


@pydra.mark.task
def afq_this(subject):
    # Create local filesystem:
    bids_path = mkdtemp(prefix=op.join(cache_dir_tmp, "bids_"))
    print(f"BIDS path is {bids_path}")
    qsiprep_path = op.join(bids_path, f"derivatives/qsiprep/sub-{subject}")

    l_dwi_path = op.join(
            qsiprep_path,
            f"sub-{subject}/dwi/")
    print(f"Creating {l_dwi_path}")
    os.makedirs(l_dwi_path)

    l_anat_path = op.join(
            qsiprep_path,
            f"sub-{subject}/anat/")
    print(f"Creating {l_anat_path}")
    os.makedirs(l_anat_path)

    to_bids_description(bids_path)
    to_bids_description(qsiprep_path, PipelineDescription={"Name": "qsiprep"})

    # Populate inputs from S3:
    bucket = "alstar2.uw.edu"
    fs = s3fs.S3FileSystem()
    exts = ['.bval', '.bvec', 'nii.gz']

    for ext in exts:
        fname = f"sub-{subject}_space-T1w_desc-preproc_dwi{ext}"
        rpath = f"{bucket}/derivatives/qsiprep/sub-{subject}/dwi/"
        lpath = op.join(l_dwi_path, fname)
        print(f"Putting {rpath} in {lpath}")
        fs.get(rpath, lpath)

    fname = f"sub-{subject}_desc-brain_mask.nii.gz"
    rpath = f"{bucket}/derivatives/qsiprep/sub-{subject}/anat/{fname}"
    lpath = op.join(l_anat_path, fname)
    print(f"Putting {rpath} in {lpath}")
    fs.get(rpath, lpath)

    # Ready to AFQ:
    brain_mask_definition = ImageFile(
        suffix="mask",
        filters={'desc': 'brain',
                 'space': 'T1w',
                 'scope': 'qsiprep'})

    tracking_params = {
        'seed_mask': ScalarImage('dki_fa'),
        'stop_mask': ScalarImage('dki_fa'),
        "odf_model": "CSD",
        "directions": "prob",
        "n_seeds": 1
    }

    scalars = ["dki_fa", "dki_md", "dki_mk", "dki_awf", "fwdti_fa", "fwdti_md", "fwdti_fwf"]

    myafq = GroupAFQ(
        bids_path=bids_path,
        preproc_pipeline='qsiprep',
        brain_mask_definition=brain_mask_definition,
        tracking_params=tracking_params,
        scalars=scalars)

    # run the AFQ objects
    print("Running the pyAFQ pipeline")
    myafq.export_all(afqbrowser=False, xforms=False)


subject_list = ["01", "02"]

t = afq_this(subject=subject_list, cache_dir=cache_dir_tmp).split("subject")
with pydra.Submitter(plugin="slurm",
                     sbatch_args="-J pyafq -p gpu-a40 -A escience --mem=58G --time=72:00:00 -o /gscratch/escience/arokem/logs/pyafq.out -e /gscratch/escience/arokem/logs/pyafq.err --mail-user=arokem@uw.edu --mail-type=ALL") as sub:
    sub(runnable=t)
