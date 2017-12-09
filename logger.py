""" LOGGING
"""
import logging
#
# CONFIG
#
FORMAT='%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)


#
# PUBLIC
#
def out(message,level='info'):
    getattr(logger,level)(message)

