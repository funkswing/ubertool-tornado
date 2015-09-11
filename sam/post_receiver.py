__author__ = 'jflaisha'



class SamPostReceiver(object):
    def __init__(self):
        pass

    def unpack(self, payload):
        import cPickle
        try:
              output = cPickle.loads(payload)
              print "Pickle Loaded"
              print type(output)
              print "-------------"

              return output
        except:
            return None