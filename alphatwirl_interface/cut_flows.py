import alphatwirl

def cut_flow_with_counter(cut_flow, cut_flow_summary_filename):
    eventSelection = alphatwirl.selection.build_selection(
        path_cfg = cut_flow,
        AllClass = alphatwirl.selection.modules.AllwCount,
        AnyClass = alphatwirl.selection.modules.AnywCount,
        NotClass = alphatwirl.selection.modules.NotwCount
    )
    resultsCombinationMethod = alphatwirl.collector.ToTupleListWithDatasetColumn(
        summaryColumnNames = ('depth', 'class', 'name', 'pass', 'total')
    )
    deliveryMethod = alphatwirl.collector.WriteListToFile(cut_flow_summary_filename)
    collector = alphatwirl.loop.Collector(resultsCombinationMethod, deliveryMethod)
    return [(eventSelection, collector)]
