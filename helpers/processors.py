from datetime import datetime
import numpy as np

DATE_STR_FMT='%Y-%m-%d'
GLAD_START_DATE_STR='2015-01-01'

class GLAD(object):

    @staticmethod
    def to_datetime(date_str):
        return datetime.strptime(date_str,DATE_STR_FMT)


    #
    # PUBLIC METHODS
    #
    def __init__(self,
            data=None,
            start_date=None,
            end_date=None):
        self._data=None
        self.glad_start_date=GLAD.to_datetime(GLAD_START_DATE_STR)
        self.start_date=start_date
        self.end_date=end_date
        self.data=self._process_data(data)


    def _process_data(self,data):
        start_days=self._date_to_days(self.start_date)
        end_days=self._date_to_days(self.end_date)
        intensity, days=self._get_intensity_days(data)
        mask=np.logical_or(days<=start_days,days>end_days)
        np.putmask(intensity,mask,0)
        return intensity


    def _get_intensity_days(self,data):
        confidence_intensity=data[:,:,2]
        days=(255 * data[:,:,0]) + data[:,:,1]
        intensity=np.mod(confidence_intensity,100)*100/55
        return intensity, days


    def _date_to_days(self,date_str):
        date=GLAD.to_datetime(date_str)
        return (date-self.glad_start_date).days





class Thresholder(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,
            data=None,
            intensity_threshold=None,
            hard_threshold=False):
        self.data=self._process_data(
            data,
            intensity_threshold,
            hard_threshold)


    def _process_data(self,data,threshold,hard):
        if threshold:
            if hard:
                data=(data>threshold).astype(int)
            else:
                np.putmask(data,data<=threshold,0)
        return data

