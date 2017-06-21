import logging
import alphatwirl
import sys


class Parallel(object):
    def __init__(self, progressMonitor, communicationChannel):
        self.progressMonitor = progressMonitor
        self.communicationChannel = communicationChannel

    def __repr__(self):
        return '{}(progressMonitor = {!r}, communicationChannel = {!r}'.format(
            self.__class__.__name__,
            self.progressMonitor,
            self.communicationChannel
        )

    def begin(self):
        self.progressMonitor.begin()
        self.communicationChannel.begin()

    def end(self):
        self.progressMonitor.end()
        self.communicationChannel.end()


def build_parallel(parallel_mode, quiet = True, n_processes = 4, user_modules = [ ], htcondor_job_desc_extra = [ ]):

    default_parallel_mode = 'multiprocessing'

    if parallel_mode in ('subprocess', 'htcondor'):
        return build_parallel_dropbox(
            parallel_mode = parallel_mode,
            quiet = quiet,
            user_modules = user_modules,
            htcondor_job_desc_extra = htcondor_job_desc_extra
        )

    if not parallel_mode == default_parallel_mode:
        logger = logging.getLogger(__name__)
        logger.warning('unknown parallel_mode "{}", use default "{}"'.format(
            parallel_mode, default_parallel_mode
        ))

    return build_parallel_multiprocessing(quiet = quiet, n_processes = n_processes)


def build_parallel_dropbox(parallel_mode, quiet, user_modules, htcondor_job_desc_extra = [ ]):
    tmpdir = '_ccsp_temp'
    user_modules = set(user_modules)
    user_modules.add('alphatwirl_interface')
    user_modules.add('alphatwirl')
    alphatwirl.mkdir_p(tmpdir)

    if quiet:
        progressMonitor = alphatwirl.progressbar.NullProgressMonitor()
    else:
        if sys.stdout.isatty():
            progressBar = alphatwirl.progressbar.ProgressBar()
        else:
            progressBar = alphatwirl.progressbar.ProgressPrint()
        progressMonitor = alphatwirl.progressbar.BProgressMonitor(presentation=progressBar)
    if parallel_mode == 'htcondor':
        dispatcher = alphatwirl.concurrently.HTCondorJobSubmitter(job_desc_extra = htcondor_job_desc_extra)
    else:
        dispatcher = alphatwirl.concurrently.SubprocessRunner()
    workingArea = alphatwirl.concurrently.WorkingArea(
        dir = tmpdir,
        python_modules = list(user_modules)
    )
    dropbox = alphatwirl.concurrently.TaskPackageDropbox(
        workingArea = workingArea,
        dispatcher = dispatcher
    )
    communicationChannel = alphatwirl.concurrently.CommunicationChannel(dropbox=dropbox)

    return Parallel(progressMonitor, communicationChannel)


def build_parallel_multiprocessing(quiet, n_processes):
    progressMonitor, communicationChannel = alphatwirl.configure.build_progressMonitor_communicationChannel(quiet = quiet, processes = n_processes)
    return Parallel(progressMonitor, communicationChannel)
