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
        """
        'document' schema = {
            "jid": jid (string),
            "output": daily_concs (np_array, multidimensional(variable)),
            "huc_ids": huc_ids (np_array)
        }
        """

        if document is not None:
            yield self.monary_setup(db, jid, document)

            # Set HTTP Status Code to 'Success: Created'
            self.set_status(201)

    # @gen.coroutine
    # def get_sim_days(self, db, jid):
    #     # Get 'sim_days' from 'metadata' document matching 'jid' of SAM run
    #     meta_doc = yield db.metadata.find_one(
    #         { "jid": jid },
    #         { "model_object_dict.sim_days": 1 }
    #     )
    #     raise gen.Return(meta_doc['model_object_dict']['sim_days'])  # Generators are not allowed to return values in Python <3.3; this is a workaround

    @gen.coroutine
    def get_sim_days(self, db, jid):
        # Get 'sim_days' from 'metadata' document matching 'jid' of SAM run
        meta_doc = yield db.metadata.find_one(
            { "jid": jid },
            { "model_object_dict.sim_days": 1 }
        )
        raise gen.Return(meta_doc['model_object_dict']['sim_days'])
        # return meta_doc['model_object_dict']['sim_days']

    @gen.coroutine
    def monary_setup(self, db, jid, document):

        day_array = yield self.get_sim_days(db, jid)
        list_of_huc_arrays = mongo_insert.extract_arrays(document['output'])
        list_of_huc_ids = document['huc_ids']

        i = 0
        while i < len(list_of_huc_arrays):  # for huc_array in list_of_huc_arrays:
            huc_id = list_of_huc_ids[i]
            print 'huc_id: ', huc_id
            sam_monary = mongo_insert.SamMonary(list_of_huc_arrays[i], day_array, huc_id)
            sam_monary.monary_insert()
            i += 1