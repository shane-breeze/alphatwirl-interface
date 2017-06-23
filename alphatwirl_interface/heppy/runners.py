from alphatwirl_interface.heppy.framework_heppy import FrameworkHeppy


def build_job_manager(outdir, heppydir, **kwargs):
    """
    Create a standard heppy job runner, which will process inputs in parallel
    on an htcondor cluster

    Parameters
    - outdir (str) --- the path to the output directory for files
    - heppydir (str) --- the path to the input heppy directory, containing the component directories
    - kwargs -- all extra arguments are passed through to the underlying FrameworkHeppy instance
    """
    htcondor_job_desc_extra_request = ['request_memory = 250']

    # # Ignore since we don't use this anyway for now
    #    # https://lists.cs.wisc.edu/archive/htcondor-users/2014-June/msg00133.shtml
    #    # hold a job and release to a different machine after a certain minutes
    #    htcondor_job_desc_extra_resubmit = [
    #        'expected_runtime_minutes = 10',
    #        'job_machine_attrs = Machine',
    #        'job_machine_attrs_history_length = 4',
    #        'requirements = target.machine =!= MachineAttrMachine1 && target.machine =!= MachineAttrMachine2 &&  target.machine =!= MachineAttrMachine3',
    #        'periodic_hold = JobStatus == 2 && CurrentTime - EnteredCurrentStatus > 60 * $(expected_runtime_minutes)',
    #        'periodic_hold_subcode = 1',
    #        'periodic_release = HoldReasonCode == 3 && HoldReasonSubCode == 1 && JobRunCount < 3',
    #        'periodic_hold_reason = ifthenelse(JobRunCount<3,"Ran too long, will retry","Ran too long")',
    #    ]
    #    # htcondor_job_desc_extra = htcondor_job_desc_extra_request + htcondor_job_desc_extra_resubmit

    # http://www.its.hku.hk/services/research/htc/jobsubmission
    # avoid the machines "smXX.hadoop.cluster"
    # operator '=!=' explained at https://research.cs.wisc.edu/htcondor/manual/v7.8/4_1HTCondor_s_ClassAd.html#ClassAd:evaluation-meta
    htcondor_job_desc_extra_blacklist = [
        'requirements=!stringListMember(substr(Target.Machine, 0, 2), "sm,bs")'
    ]

    htcondor_job_desc_extra = htcondor_job_desc_extra_request + htcondor_job_desc_extra_blacklist

    heppy_mgr = FrameworkHeppy(
        outdir = outdir,
        heppydir = heppydir,
        htcondor_job_desc_extra = htcondor_job_desc_extra,
        **kwargs

    )
    return heppy_mgr
