import numpy as np


class GLAD(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,
            data=None,
            start_date=None,
            end_date=None):
        self._data=None
        self.raw_data=data
        self.start_date=start_date
        self.end_date=end_date


    def data(self):
        print('FAKE DATA',self.raw_data[:,:,2])
        if not self._data:
            self._data=self._process_data()
        return self._data


    def _process_data(self):
        # USE DATES TO FILTER GLAD DATA
        # 
        # FAKE VERSION
        data=self.raw_data[:,:,2]
        return data




class Thresholder(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,
            data=None,
            intensity_threshold=None,
            hard_threshold=False):
        self._data=None
        self.raw_data=data
        self.intensity_threshold=intensity_threshold
        self.hard_threshold=hard_threshold


    def data(self):
        if not self._data:
            self._data=self.raw_data.copy()
            if self.intensity_threshold:
                if self.hard_threshold:
                    self._data=(self._data>self.intensity_threshold).astype(int)
                else:
                    np.putmask(self._data,self._data<=self.intensity_threshold,0)
        return self._data

