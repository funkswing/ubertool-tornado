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
    def __init__(self, huc_output_array, day_array, huc_id):
        """
        Class represents each HUC worth of output data from SuperPRZM run.  The class methods take the numpy array
        SuperPRZM output data and convert them to MonaryParams to be inserted into MongoDB using Monary.
        :param huc_output_array: numpy array, SuperPRZM output data for one HUC
        :param day_array: numpy array, sequence of "Julian Days" of simulation date range
        :param huc_id: string, HUC12 ID (12 digits)
        """
        len_huc_output_array = len(huc_output_array)
        len_day_array = len(day_array)
        if len_huc_output_array is not len_day_array:
            # Delete excess indices in numpy array (numpy array length is greater than total number of simulation days)
            # (output arrays in Fortran have len on x-axis (axis=0) set to 11323, which is 10958 + 365)
            self.huc_output_array = np.delete(huc_output_array,
                                              [len_day_array + x for x in range(len_huc_output_array - len_day_array)],
                                              0)
            if len(self.huc_output_array) is not len(day_array):
                raise ValueError("NumPy arrays must be equal in length")
        else:
            self.huc_output_array = huc_output_array

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
        array_size = len(self.huc_output_array)
        masked_out = np.ma.masked_array(
            # Output data array
            self.huc_output_array,  # array containing Fortran output
            np.zeros(array_size, dtype=np.ma.nomask)  # Special value indicating masking is not needed (increases speed)
        ), np.ma.masked_array(
            # array of sim_days
            self.day_array,
            np.zeros(array_size, dtype=np.ma.nomask)
        ), np.ma.masked_array(
            # array of HUC_ID (repeating same value) of which the Fortran output is associated
            np.full(array_size, self.huc_id, dtype='|S12'),
            np.zeros(array_size, dtype=np.ma.nomask)
        )

        return masked_out

    def monary_insert(self):
        """
        Creates and inserts the MonaryParam into MongoDB
        :return: numpy array of the Mongo ObjectIDs of the inserted documents
        """
        ma_data, ma_day, ma_huc_id = self.create_masked_array()
        m_params = self.create_monary_params(ma_data, ma_day, ma_huc_id)

        return client.insert("sam", "daily", m_params)
