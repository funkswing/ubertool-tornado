__author__ = 'jflaisha'

import json
from bson import json_util
from tornado import gen
from tornado.web import RequestHandler, asynchronous
import post_receiver as sam_handler
import mongo_insert


class SamMetaDataHandler(RequestHandler):
    """
    /sam/metadata/<jid>
    """


    def __schema__(self):
        """
        Schema of the Metadata documents
        :return: string
        """
        return """
        {
        "user_id": "admin",
        "jid": string,
        "run_type": string,
        "model_object_dict": {
            'filename': string,
            'input': {k: v,...},
            'sim_days': list
        }
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
        document = yield db_sam.metadata.find_one({"jid": jid})
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(document, default=json_util.default))

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


    def __schema__(self):
        """
        Schema of the Metadata documents
        :return: string
        """
        return """
        {
        '_id': ObjectID()
        'data': float
        'day': int
        'huc_id': string
        }
        """

    @asynchronous
    @gen.coroutine
    def get(self, jid):
        print jid
        db = self.settings['db_sam']
        document = yield db.sam.find_one({ "jid": jid })
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(document, default=json_util.default))

    @asynchronous
    @gen.coroutine
    def post(self, jid):
        """
        :param jid: string, Job ID used as a unique identifier for a SAM run
        """
        db = self.settings['db_sam']
        sam = sam_handler.SamPostReceiver()

        document = sam.unpack(self.request.body)

        if document is not None:
            day_array = self.get_sim_days(db, jid)

            list_of_huc_arrays = mongo_insert.extract_arrays(document['output'])
            list_of_huc_ids = document['huc_ids']

            for huc_array in list_of_huc_arrays:
                huc_id = list_of_huc_ids.index(huc_array)
                mongo_insert.SamMonary(huc_array, day_array, huc_id)

            # Set HTTP Status Code to 'Success: Created'
            self.set_status(201)

    @gen.coroutine
    def get_sim_days(self, db, jid):
        # Get 'sim_days' from Metadata document matching 'jid' of SAM run
        meta_doc = yield db.metadata.find_one(
            { "jid": jid },
            { "model_object_dict.sim_days": 1 }
        )
        raise gen.Return(meta_doc['model_object_dict']['sim_days'])
