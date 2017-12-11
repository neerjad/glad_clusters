from os import environ
import __builtin__

#
#  ENV/CONFIG
#
def get(var_name,default=False,typ=None,required=True):
    if required and (default is False):
        val=environ[var_name]
    else:
        val=environ.get(var_name,default)
    val=_strtoval(val)
    if val and typ:
        val=getattr(__builtin__,typ)(val)
    return val


def str(var_name,default=None,required=False):
    return get(var_name,default,required)


def int(var_name,default=None,required=False):
    return get(var_name,default=default,typ='int',required=required)


def float(var_name,default=None,required=False):
    return get(var_name,default=default,typ='float',required=required)


def bool(var_name,default=None,required=False):
    return get(var_name,default=default,typ='bool',required=required)


def _strtoval(string):
    if string:
        lc_string=string.lower()
        if lc_string=='false':
            return False
        elif lc_string=='true':
            return True
        elif lc_string=='none':
            return None
        else:
            return string