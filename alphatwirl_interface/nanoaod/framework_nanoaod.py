# Tai Sakuma <tai.sakuma@cern.ch>
import os
import sys
import logging

import alphatwirl

##__________________________________________________________________||
import logging
logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(stream=sys.stdout)
log_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

##__________________________________________________________________||
from parallel import build_parallel
from profile_func import profile_func

##__________________________________________________________________||
class FrameworkNanoAOD(object):
    """A simple framework for using alphatwirl

    Args:
        outdir (str): the output directory
        force (bool): overwrite the output if True
        quiet (bool): don't show progress bars if True
        parallel_mode (str): 'multiprocessing', 'subprocess', 'htcondor'
        n_processes (int): the number of processes for the 'multiprocessing' mode
        user_modules (list of str): names of python modules to be copied for the 'subprocess' mode
        max_events_per_dataset (int):
        max_events_per_process (int):
        max_files_per_run (int):
        profile (bool): run cProfile if True
        profile_out_path (bool): path to store the result of the profile. stdout if None

    """
    def __init__(self, outdir,
                 force = False, quiet = False,
                 parallel_mode = 'multiprocessing',
                 htcondor_job_desc_extra = [ ],
                 n_processes = 8,
                 user_modules = (),
                 max_events_per_dataset = -1, max_events_per_process = -1,
                 max_files_per_run = 1,
                 profile = True, profile_out_path = "prof.txt", #None
    ):
        self.parallel = build_parallel(
            parallel_mode = parallel_mode,
            quiet = quiet,
            n_processes = n_processes,
            user_modules = user_modules,
            htcondor_job_desc_extra = htcondor_job_desc_extra,
        )
        self.outdir = outdir
        self.force =  force
        self.max_events_per_dataset = max_events_per_dataset
        self.max_events_per_process = max_events_per_process
        self.max_files_per_run = max_files_per_run
        self.profile = profile
        self.profile_out_path = profile_out_path

    def run(self,
            reader_collector_pairs,
            components=None,
            tree_name='Events'
    ):

        self._begin()
        try:
            loop = self._configure(components, reader_collector_pairs, tree_name)
            ret_val = self._run(loop)
        except KeyboardInterrupt:
            logger = logging.getLogger(__name__)
            logger.warning('received KeyboardInterrupt')
            ret_val = ["Interrupted"]
            pass
        self._end()
        return ret_val

    def _begin(self):
        self.parallel.begin()

    def _configure(self, components, reader_collector_pairs, tree_name):

        component_readers = alphatwirl.nanoaod.ComponentReaderComposite()

        # # tbl_heppyresult.txt
        # tbl_heppyresult_path = os.path.join(self.outdir, 'tbl_heppyresult.txt')
        # if self.force or not os.path.exists(tbl_heppyresult_path):
        #     # e.g., '74X/MC/20150810_MC/20150810_SingleMu'
        #     heppydir_rel = '/'.join(self.heppydir.rstrip('/').split('/')[-4:])
        #     alphatwirl.mkdir_p(os.path.dirname(tbl_heppyresult_path))
        #     f = open(tbl_heppyresult_path, 'w')
        #     f.write('heppyresult\n')
        #     f.write(heppydir_rel + '\n')
        #     f.close()

        # # tbl_dataset.txt
        # tbl_dataset_path = os.path.join(self.outdir, 'tbl_dataset.txt')
        # if self.force or not os.path.exists(tbl_dataset_path):
        #     tblDataset = alphatwirl.heppyresult.TblComponentConfig(
        #         outPath = tbl_dataset_path,
        #         columnNames = ('dataset', ),
        #         keys = ('dataset', ),
        #     )
        #     component_readers.add(tblDataset)

        # # tbl_xsec.txt for MC
        # if not self.isdata:
        #     tbl_xsec_path = os.path.join(self.outdir, 'tbl_xsec.txt')
        #     if self.force or not os.path.exists(tbl_xsec_path):
        #         tblXsec = alphatwirl.heppyresult.TblComponentConfig(
        #             outPath = tbl_xsec_path,
        #             columnNames = ('xsec', ),
        #             keys = ('xSection', ),
        #         )
        #         component_readers.add(tblXsec)

        # # tbl_nevt.txt for MC
        # if not self.isdata:
        #     tbl_nevt_path = os.path.join(self.outdir, 'tbl_nevt.txt')
        #     if self.force or not os.path.exists(tbl_nevt_path):
        #         tblNevt = alphatwirl.heppyresult.TblCounter(
        #             outPath = tbl_nevt_path,
        #             columnNames = ('nevt', 'nevt_sumw'),
        #             analyzerName = 'skimAnalyzerCount',
        #             fileName = 'SkimReport.txt',
        #             levels = ('All Events', 'Sum Weights')
        #         )
        #         component_readers.add(tblNevt)

        # event loop
        reader = alphatwirl.loop.ReaderComposite()
        collector = alphatwirl.loop.CollectorComposite(self.parallel.progressMonitor.createReporter())
        for r, c in reader_collector_pairs:
            reader.add(r)
            collector.add(c)
        event_loop_runner = alphatwirl.loop.MPEventLoopRunner(self.parallel.communicationChannel)
        event_builder_config_maker = alphatwirl.nanoaod.EventBuilderConfigMaker(
            treeName = tree_name,
        )
        dataset_into_event_builders_splitter = alphatwirl.loop.DatasetIntoEventBuildersSplitter(
            EventBuilder = alphatwirl.nanoaod.EventBuilder,
            eventBuilderConfigMaker = event_builder_config_maker,
            maxEvents = self.max_events_per_dataset,
            maxEventsPerRun = self.max_events_per_process,
            maxFilesPerRun = self.max_files_per_run,
        )
        event_reader = alphatwirl.loop.EventsInDatasetReader(
            eventLoopRunner = event_loop_runner,
            reader = reader,
            collector = collector,
            split_into_build_events = dataset_into_event_builders_splitter
        )
        component_readers.add(event_reader)

        #if components == ['all']: components = None
        nanoaod_result = alphatwirl.nanoaod.NanoAODResult(
            component_df = components,
        )
        component_loop = alphatwirl.nanoaod.ComponentLoop(nanoaod_result, component_readers)

        return component_loop

    def _run(self, component_loop):
        if not self.profile:
            ret_val = component_loop()
        else:
            ret_val = profile_func(func = component_loop, profile_out_path = self.profile_out_path)

        # Only the last entry in the list of component_readers will be the user-requested data
        # Other entries are the other tables that we create to help book-keeping, and which are saved to file automatically
        return ret_val[-1]

    def _end(self):
        self.parallel.end()

##__________________________________________________________________||
