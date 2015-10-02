__author__ = 'jflaisha'

from monary import Monary, MonaryParam
import numpy as np
import requests


client = Monary(database="sam")  # defaults to localhost:27017

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

    def create_monary_params(self, ma_data, ma_day, ma_huc_id):
        """
        Create MonaryParams for inserting numpy arrays into Mongo
        :param ma_data: numpy masked array,
        :param ma_day: numpy masked array,
        :param ma_huc_id: numpy masked array,
        :return: MonaryParam object
        """
        return MonaryParam.from_list(
            [ma_data, ma_day, ma_huc_id],  # NumPy array(s)
            ['data', 'day', 'huc_id']  # Name of the field (will be the key in MongoDB)
        )

    def create_masked_array(self):
        """
        Create numpy masked array from ndarray output of SAM run.  The masked array is rank 3, with each rank (row)
        having equal numpy of indices (number of simulation days):
            [daily conc. output]
        :return: numpy masked array, output data (float32), sim_days (int64), & HUC_ID, repeating, (|S12)
        """
        array_size = len(self.huc_array)
        masked_out = np.ma.masked_array(  # Output data array
            self.huc_array,  # array containing Fortran output
            np.zeros(array_size, dtype=np.ma.nomask)  # Special value indicating masking is not needed (increases speed)
        ), np.ma.masked_array(  # array of sim_days
            self.day_array,
            np.zeros(array_size, dtype=np.ma.nomask)
        ), np.ma.masked_array(  # array of HUC_ID (repeating) of which the Fortran output is associated
            np.full(array_size, self.huc_id, dtype='|S12'),
            np.zeros(array_size, dtype=np.ma.nomask)
        )

        return masked_out

    def monary_insert(self):
        """
        Inserts the MonaryParam into MongoDB
        :return: numpy array of the Mongo ObjectIDs of the inserted documents
        """
        ma_data, ma_day, ma_huc_id = self.create_masked_array()
        m_params = self.create_monary_params(ma_data, ma_day, ma_huc_id)

        return client.insert("sam", "daily", m_params)
