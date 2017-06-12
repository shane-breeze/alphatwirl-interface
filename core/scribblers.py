import numpy as np

class ComponentName(object):

    def __repr__(self):
        return '{}()'.format(
            self.__class__.__name__,
        )

    def begin(self, event):
        self.vals = [ ]
        event.componentName = self.vals

        self.vals[:] = [event.component.name]
        # e.g., "HTMHT_Run2015D_PromptReco_25ns"

    def event(self, event):
        event.componentName = self.vals


class FuncOnNumpyArrays(object):
    def __init__(self, src_arrays, out_name, func):
        self.src_arrays = src_arrays
        self.out_name = out_name
        self.func = func

    def __repr__(self):
        name_value_pairs = (
            ('src_arrays', self.src_arrays),
            ('out_name', self.out_name),
            ('func', self.func),
        )
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join(['{} = {!r}'.format(n, v) for n, v in name_value_pairs]),
        )

    def begin(self, event):
        self.out = [ ]
        self._attach_to_event(event)

    def _attach_to_event(self, event):
        setattr(event, self.out_name, self.out)

    def event(self, event):
        self._attach_to_event(event)
        self.out[:] = self.func(*[np.array(getattr(event, n)) for n in self.src_arrays])


class DivideNumpyArrays(FuncOnNumpyArrays):
    def __init__(self, src_arrays, out_name):
        super(self.__class__, self).__init__(src_arrays, out_name, np.divide)

