from datetime import datetime, timedelta
import numpy as np


DATE_STR_FMT='%Y-%m-%d'
INT_DATE_FMT='%Y%m%d'
GLAD_START_DATE=datetime.strptime('2015-01-01',DATE_STR_FMT)


#
# GLAD ALERTS
#
def glad_between_dates(
        data,
        start_date,
        end_date,
        return_days=True,
        return_intensity=False):
    """ filter glad image by dates

        Args:
            data<arr>: glad image
            start_date<str>: yyyy-mm-dd
            end_date<str>: yyyy-mm-dd
            return_days<bool|True>: if true return days-since band
            return_intensity<bool|False>: if true return intensity bands

        Returns:
            image with requested bands. note: if both return_days
            and return_intensity are false it will return an image
            with 1 if an alert exists between the dates, otherwise 0.
    
    """
    intensity,days=_get_intensity_days(data)
    is_between_dates=_days_are_between_dates(days,start_date,end_date)
    if return_days:
        if return_intensity:
            bands=[_between_dates(is_between_dates,intensity),
                   _between_dates(is_between_dates,days)]
            im=np.dstack(bands)
        else:
            im=_between_dates(is_between_dates,days)
    elif return_intensity:
        im=_between_dates(is_between_dates,intensity)
    else:
        im=is_between_dates.astype(int)
    return im


def date_for_days(days):
    date=(GLAD_START_DATE+timedelta(days=days))
    return int(date.strftime(INT_DATE_FMT))


def _between_dates(is_between_dates,im):
    return np.where(is_between_dates,im,0)


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




#
# THRESHOLD DATA
#
def threshold(data,threshold=0,hard_threshold=False):
    """
        if hard_threshold return 1|0 for over|under threshold
        otherwise set all data under threshold to zero     
    """
    test=(data>threshold)
    if hard_threshold:
        return test.astype(float)
    else:
        return np.where(test,data,0.0)

