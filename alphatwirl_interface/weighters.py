import operator 


class WeightCalculatorProduct(object):
    """
    return the product of multiple weights
    """
    def __init__(self, weight_list):
        self.weight_list = weight_list

    def __repr__(self):
        name_value_pairs = (
            ('weight_list', self.weight_list),
        )
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join(['{} = {!r}'.format(n, v) for n, v in name_value_pairs]),
        )

    def __call__(self, event):
        weights = [getattr(event, attr)[0] for attr in self.weight_list]
        return reduce(operator.mul, weights, 1)
