
import os
import os.path as op
import s3fs
from tempfile import mkdtemp
import glob
from datetime import datetime

import pydra

from AFQ.api.group import GroupAFQ
from AFQ.definitions.image import ImageFile, ScalarImage
from AFQ.definitions.mapping import ItkMap
from AFQ.data.fetch import to_bids_description


scratch_dir = "/gscratch/escience/arokem/"
scratch_dir_tmp = op.join(scratch_dir, "tmp_")
cache_dir_tmp = mkdtemp(prefix=scratch_dir_tmp)
today = datetime.today().strftime('%Y-%m-%d')

@pydra.mark.task
def afq_this(subject):
    # Create local filesystem:
    bids_path = op.join("/gscratch/escience/arokem/pyafq_data/", f"bids_sub-{subject}")
    os.makedirs(bids_path)
    print(f"BIDS path is {bids_path}")
    qsiprep_path = op.join(bids_path, "derivatives/qsiprep/")

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

    to_bids_description(bids_path,
                        Name=f"sub-{subject}")
    to_bids_description(qsiprep_path,
                        Name=f"sub-{subject}",
                        PipelineDescription={"Name": "qsiprep"})

    # Populate inputs from S3:
    bucket = "alstar2.uw.edu"
    fs = s3fs.S3FileSystem()
    exts = ['.bval', '.bvec', '.nii.gz']

    for ext in exts:
        fname = f"sub-{subject}_space-T1w_desc-preproc_dwi{ext}"
        rpath = f"{bucket}/derivatives/qsiprep/sub-{subject}/dwi/{fname}"
        if not op.exists(l_dwi_path):
            print(f"Putting {rpath} in {l_dwi_path}")
            fs.get(rpath, l_dwi_path)

    fname = f"sub-{subject}_desc-brain_mask.nii.gz"
    rpath = f"{bucket}/derivatives/qsiprep/sub-{subject}/anat/{fname}"
    if not op.exists(l_anat_path):
        print(f"Putting {rpath} in {l_anat_path}")
        fs.get(rpath, l_anat_path)

    # Ready to AFQ:
    brain_mask_definition = ImageFile(
        suffix="mask",
        filters={'desc': 'brain',
                 'scope': 'qsiprep'})

    tracking_params = {
        'seed_mask': ScalarImage('dki_fa'),
        'stop_mask': ScalarImage('dki_fa'),
        "seed_threshold": 0.3,
        "odf_model": "CSD",
        "directions": "prob",
        "n_seeds": 2
    }

    scalars = ["dki_fa", "dki_md", "dki_mk", "dki_awf",
               "fwdti_fa", "fwdti_md", "fwdti_fwf"]

    myafq = GroupAFQ(
        bids_path=bids_path,
        preproc_pipeline='qsiprep',
        brain_mask_definition=brain_mask_definition,
        tracking_params=tracking_params,
        scalars=scalars)

    # run the AFQ objects
    print("Running the pyAFQ pipeline")
    myafq.export_all(afqbrowser=False, xforms=False)

    # Both in the directory and nested directories:
    for lpath in glob.glob(op.join(bids_path, f"derivatives/afq/sub-{subject}", "*")):
        rpath = op.join(f"{bucket}/derivatives/afq{today}/sub-{subject}/",
                        op.split(lpath)[-1])
        if not fs.exists(rpath):
            print(f"Putting {lpath} in {rpath}")
            fs.put(lpath, rpath)

    for lpath in glob.glob(op.join(bids_path, f"derivatives/afq/sub-{subject}", "*", "*")):
        rpath = op.join(f"{bucket}/derivatives/afq{today}/sub-{subject}/",
                        op.split(lpath)[-1])
        if not fs.exists(rpath):
            print(f"Putting {lpath} in {rpath}")
            fs.put(lpath, rpath)


subject_list = [f"{ii:02}" for ii in range(1, 70)]
t = afq_this(subject=subject_list, cache_dir=cache_dir_tmp).split("subject")

with pydra.Submitter(plugin="slurm",
                     sbatch_args="-J pyafq -p gpu-a40 -A escience --mem=58G --time=12:00:00 -o /gscratch/escience/arokem/logs/pyafq.out -e /gscratch/escience/arokem/logs/pyafq.err --mail-user=arokem@uw.edu --mail-type=ALL") as sub:
    sub(runnable=t)
