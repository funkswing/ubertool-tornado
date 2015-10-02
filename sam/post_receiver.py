__author__ = 'jflaisha'

import cPickle

class SamPostReceiver(object):
    def __init__(self):
        pass

    def unpack(self, payload):

        try:
            return cPickle.loads(payload)
        except:
            return None

    def pack_binary(self, np_array):
        from bson.binary import Binary
        import warnings
        warnings.warn('This POST method is deprecated, as it stores NumPy objects as binary blobs', DeprecationWarning)
        return Binary(cPickle.dumps(np_array, protocol=2))
