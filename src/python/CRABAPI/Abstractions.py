""" Task - top-level Task class """
import CRABAPI.TopLevel
import CRABClient.Commands.submit
from WMCore.Configuration import Configuration
class Task(object):
    """
        Task - Wraps methods and attributes for a single analysis task
    """
    def __init__(self, submitClass = CRABClient.Commands.submit.submit):
        self.config = Configuration()
        self.apiLog, self.clientLog, self.tracebackLog = \
                CRABAPI.TopLevel.getAllLoggers()
        self.submitClass = submitClass

    def submit(self):
        """
            submit - Sends the current task to the server. Returns requestID
        """
        args = ['-c', self.config, '--proxy', '1']
        submitCommand = self.submitClass(self.clientLog, args)
        retval = submitCommand()
        print "retval was %s" % retval
        return retval['uniquerequestname']

    def kill(self):
        """
            kill - Tells the server to cancel the current task
        """
        raise NotImplementedError

    def __getattr__(self, name):
        """
            __getattr__ - expose certain values as attributes
        """
        if name == 'jobs':
            raise NotImplementedError
        else:
            raise AttributeError
