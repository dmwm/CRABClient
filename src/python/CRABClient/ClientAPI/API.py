import logging
from CRABClient.client_utilities import getLoggers
from WMCore.Configuration import Configuration, ConfigSection

def cmdexec(command, *args, **kwargs):
    logger, _ = getLoggers(logging.INFO)

    #if the user provide a class config rather than a config python file
    if 'config' in kwargs.keys() and isinstance(kwargs['config'], Configuration):
        cmdarg = ['--config' , kwargs['config']]
        if len(args) != 0:
            cmdarg.extend(args)
    else:
        #all kwargs argument have to be transformed into a list, first white space is removed, then '--' and '=' is added to kwargs.keys
        #and kwargs value respectively. Then joined
        cmdarg = [''.join(('--'+str(arg)+'='+str(kwargs[arg])).split()) for arg in kwargs]
        cmdarg.extend(args)

    try:
        mod = __import__('CRABClient.Commands.%s' % command, fromlist=command)
    except ImportError:
        raise Exception('Wrong crab command give, command give: %s' % command)

    try:
        cmdobj = getattr(mod, command)(logger , cmdarg)
        res = cmdobj()
    except SystemExit, se:
        #most likely an error from the OptionParser in Subcommand. Can't avoid a sys.exit from there but can capture it so we don't exit in a bad way
        if se.code==2:
            raise Exception("Captured a system exit. Probably caused by a wrong option passed to the command.")

    return res


class Task(object):
    """
        Task - Wraps methods and attributes for a single analysis task
    """
    def __init__(self):
        self.config = Configuration()
#        self.apiLog, self.clientLog, self.tracebackLog = \
#                CRABAPI.TopLevel.getAllLoggers()

    def submit(self):
        """
            submit - Sends the current task to the server. Returns requestID
        """
        args = [ '--skip-proxy','unittest-noproxy' ]
        res = cmdexec('submit', config=self.config)
        return res

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
