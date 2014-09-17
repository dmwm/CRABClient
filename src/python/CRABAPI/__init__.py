""" CRABAPI - https://github.com/PerilousApricot/CRABAPI
        The outward-facing portion of the API
"""

from CRABAPI.TopLevel import getTask, setLogging, getAllLoggers, getLogger

# Make sense of CRABClient's exceptions by making an exception tree
class APIException(Exception):
    """
        APIException - top of the CRABAPI exception tree
    """
    pass

class BadArgumentException(APIException):
    """
        BadArgumentException - Arguments passed didn't pass optparse's muster
    """
    pass


def setUpPackage():
    """ Need to make sure logging is initialized before any tests run. This
        should NOT be called by client functions, it is used by the testing
        suite """
    import logging
    setLogging(logging.DEBUG, logging.DEBUG, logging.DEBUG)

# Used if someone does an "import * from CRABAPI"
from CRABAPI.Abstractions import Task
from CRABAPI.RawCommand import execRaw
__all__ = ["getTask", "setLogging", "getAllLoggers", "getLogger", "Task", \
           "execRaw"]
