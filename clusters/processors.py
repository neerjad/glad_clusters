from datetime import datetime
import numpy as np


DATE_STR_FMT='%Y-%m-%d'
GLAD_START_DATE=datetime.strptime('2015-01-01',DATE_STR_FMT)


"""glad_between_dates

"""
def glad_between_dates(data,start_date,end_date,intensity_only=True):
    intensity, days=_get_intensity_days(data)
    days_test=_days_are_between_dates(days,start_date,end_date)
    if intensity_only:
        return np.where(days_test,intensity,0)
    else:
        days_test=np.dstack([days_test,days_test,days_test])
        return np.where(days_test,data,0)


def _days_are_between_dates(days,start_date,end_date):
    start_days=_days_since_glad_start(start_date)
    end_days=_days_since_glad_start(end_date)
    return np.logical_and(days>=start_days,days<end_days)


def _days_since_glad_start(date_str):
    date=datetime.strptime(date_str,DATE_STR_FMT)
    return (date-GLAD_START_DATE).days


def _get_intensity_days(data):
    confidence_intensity=data[:,:,2]
    days=(255.0 * data[:,:,0]) + data[:,:,1]
    intensity=np.mod(confidence_intensity,100.0)*100.0/55
    return intensity, days






"""threshold

"""
def threshold(data,threshold=0,hard_threshold=False):
    test=(data>threshold)
    if hard_threshold:
        return test.astype(float)
    else:
        return np.where(test,data,0.0)

