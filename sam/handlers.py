__author__ = 'jflaisha'

import json
from bson import json_util
from tornado import gen
from tornado.web import RequestHandler, asynchronous
import post_receiver as sam_handler


class SamMetaDataHandler(RequestHandler):
    """
    /sam/metadata/<jid>
    """
    @asynchronous
    @gen.coroutine
    def get(self, jid):
        """
        Query metadata from Mongo for 'jid'.  Metadata includes inputs,
        username, run type (single, batch, qaqc), and simulation days.

        :param jid: string, Job ID used as a unique identifier for a SAM run
        """
        db_sam = self.settings['db_sam']
        document = yield db_sam.metadata.find_one({ "jid": jid })
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps((document),default=json_util.default))

    @asynchronous
    @gen.coroutine
    def post(self, jid):
        """
        Insert metadata into Mongo for 'jid'.  Metadata includes inputs,
        username, run type (single, batch, qaqc), and simulation days.

        :param jid: string, Job ID used as a unique identifier for a SAM run
        """
        db_sam = self.settings['db_sam']
        document = json.loads(self.request.body)

        yield db_sam.metadata.insert(document)
        self.set_header("Content-Type", "application/json")
        self.set_status(201)

class SamDailyHandler(RequestHandler):
    """
    /sam/daily/<jid>
    """
    @asynchronous
    @gen.coroutine
    def get(self, jid):
        print jid
        db = self.settings['db_sam']
        document = yield db.sam.find_one({ "jid": jid })
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps((document),default=json_util.default))

    @asynchronous
    @gen.coroutine
    def post(self, jid):
        """
        DEPRECATED: Monary is now used for inserting NumPy objects into Mongo

        :param jid: string, Job ID used as a unique identifier for a SAM run
        """
        db = self.settings['db_sam']
        sam = sam_handler.SamPostReceiver()
        try:
            document = json.loads(self.request.body)
            print 'JSON'
        except ValueError:
            document = sam.unpack(self.request.body)
            if document is not None:
                # document['model_object_dict']['output'] = 'numpy placeholder'  # Dummy NumPy data

                # DEPRECATED: puts NumPy array into Mongo as binary blob
                document['model_object_dict']['output'] = sam.pack_binary(document['model_object_dict']['output'])

                # sim_days ###########################

        yield db.sam.insert(document)
        self.set_header("Content-Type", "application/json")
        self.set_status(201)

        import warnings
        warnings.warn('This POST method is deprecated, as it stores NumPy objects as binary blobs', DeprecationWarning)