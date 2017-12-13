import imageio as io
import numpy as np


class GLAD(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,
            data_path,
            start_date=None,
            end_date=None,
            intensity_threshold=None,
            hard_threshold=False):
        self._data=None
        self.data_path=data_path
        self.raw_data=io.imread(data_path)
        self.start_date=start_date
        self.end_date=end_date
        self.intensity_threshold=intensity_threshold
        self.hard_threshold=hard_threshold


    def data(self):
        print('DATA',self.raw_data)
        if not self._data:
            self._data=self._process_data()
        return self._data


    def _process_data(self):
        # USE DATES TO FILTER GLAD DATA
        # 
        # FAKE VERSION
        data=self.raw_data[:,:,0]
        if self.intensity_threshold:
            if self.hard_threshold:
                data=(data>self.intensity_threshold).astype(int)
            else:
                np.putmask(data,data<=self.intensity_threshold,0)
        return data

