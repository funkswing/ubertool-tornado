__author__ = 'jflaisha'

import cPickle

class SamPostReceiver(object):
    def __init__(self):
        pass

    def unpack(self, payload):

        try:
              output = cPickle.loads(payload)
              print "Pickle Loaded"
              print type(output)
              print "-------------"

              return output
        except:
            return None

    def pack_binary(self, np_array):
        from bson.binary import Binary

        return Binary(cPickle.dumps(np_array, protocol=2))