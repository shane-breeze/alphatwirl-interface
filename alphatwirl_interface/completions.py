import alphatwirl

def complete(df_configs, completer):
        df_configs = [completer.complete(c) for c in df_configs]
        reader_collector_pairs = [build_counter_collector_pair(config) for config in df_configs]
        return reader_collector_pairs

def build_counter_collector_pair(df_cfg):
    keyValComposer = alphatwirl.summary.KeyValueComposer(
        keyAttrNames = df_cfg['keyAttrNames'],
        binnings = df_cfg['binnings'],
        keyIndices = df_cfg['keyIndices'],
        valAttrNames = df_cfg['valAttrNames'],
        valIndices = df_cfg['valIndices']
    )
    nextKeyComposer = alphatwirl.summary.NextKeyComposer(df_cfg['binnings']) if df_cfg['binnings'] is not None else None
    summarizer = alphatwirl.summary.Summarizer(
        Summary = df_cfg['summaryClass']
    )
    reader = alphatwirl.summary.Reader(
        keyValComposer = keyValComposer,
        summarizer = summarizer,
        nextKeyComposer = nextKeyComposer,
        weightCalculator = df_cfg['weight'],
        nevents = df_cfg['nevents']
    )
    resultsCombinationMethod = alphatwirl.collector.ToDataFrame(
        summaryColumnNames = df_cfg['keyOutColumnNames'] + df_cfg['valOutColumnNames']
    )
    deliveryMethod = alphatwirl.collector.WritePandasDataFrameToFile(df_cfg['outFilePath'])
    collector = alphatwirl.loop.Collector(resultsCombinationMethod, deliveryMethod)
    return reader, collector

def to_null_collector_pairs(analyzers):
    ret = [(r, alphatwirl.loop.NullCollector()) for r in analyzers]
    return ret

