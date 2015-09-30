__author__ = 'jflaisha'


from monary import Monary, MonaryParam
import numpy as np
client = Monary(database="sam")  # defaults to localhost:27017

""" SCHEMA:
    "user_id": "admin",
    "jid": name_temp + "_" + section,
    "run_type": 'single',
    "model_object_dict": {
        #'filename': filename,
        #'input': args
        'output': np_array
    }
"""

def extract_arrays(fortran_output):
    """
    Generate list containing a tuple for each HUC in a SAM run.  The output from the Fortran is a 2-dimensional
    array where the y-axis represents each HUC in the run.  The x-axis is a series of output values, with the first
    value being the HUC_ID and the remaining a time-series of output values.

    :param fortran_output: numpy.ndarray
    :return: list of tuples (tuple[0] = HUC_ID (str), tuple[1] = Output values (numpy.ndarray))
    """
    # return list(array for array in self.fortran_output)
    # return [array for array in self.fortran_output]
    return [array for array in fortran_output]


class SamMonary(object):
    def __init__(self, huc_array, day_array, huc_id):

        if len(huc_array) is not len(day_array):
            raise ValueError("NumPy arrays must be equal in length")

        self.huc_array = huc_array
        self.day_array = day_array
        self.huc_id = huc_id

    def create_params(self, ma_data, ma_day, ma_huc_id):
        """

        :param output_array:
        :return:
        """
        return MonaryParam.from_list(
            [ma_data, ma_day, ma_huc_id],  # NumPy array(s)
            ['data', 'day', 'huc_id']  # Name of the field (will be the key in MongoDB)
        )

    def create_masked_array(self):
        """

        :param np_array:
        :return:
        """
        return np.ma.masked_array(  # Output data array
            self.huc_array,  # NumPy array containing Fortran output
            np.zeros(len(self.huc_array), dtype=np.ma.nomask)  # Special value indicating masking is not needed (increases speed)
        ), np.ma.masked_array(  #
            self.day_array,
            np.zeros(len(self.day_array), dtype=np.ma.nomask)
        ), np.ma.masked_array(  # HUC_ID array (repeating)
            np.full(len(self.huc_array), self.huc_id, dtype='|S12'),
            np.zeros(len(self.huc_array), dtype=np.ma.nomask)
        )

    def monary_insert(self):
        ma_data, ma_day, ma_huc_id = self.create_masked_array()
        m_params = self.create_params(ma_data, ma_day, ma_huc_id)

        return client.insert("sam", "daily", m_params)
