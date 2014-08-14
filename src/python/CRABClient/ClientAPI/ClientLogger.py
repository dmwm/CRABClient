import logging
import sys


class ClientLogger():

    def chkinstance(self , cmdobj):

        if hasattr(cmdobj, 'logger'):
            return cmdobj.logger
        else:
            raise Exception('Object has no logger attribute')

    def add(self , cmdobj,  logname = '', loglevel = 'info', logdestination = None ):

        loginstace = self.chkinstance(cmdobj)

        if not hasattr(loginstace , 'logdic'):
            loginstace.logdic= {}

        acceptedlevel = ['info' , 'quiet' , 'debug']

        if logname is '' or logname in loginstace.logdic:
            raise Exception('logname value is None or already being used')
        elif loglevel not in acceptedlevel:
            raise Exception('Only this level is accepted : %s' %acceptedlevel)

        if logdestination is None:
            d = {'handler': logging.StreamHandler(sys.stdout) , 'destination': 'sys.stdout' , 'level' : loglevel}
            loginstace.logdic[logname] = d
        else:
            try:
                d = {'handler' : logging.FileHandler(logdestination) , 'destination' : logdestination , 'level' : loglevel}
                loginstace.logdic[logname] = d
            except IOError:
                raise IOError('Failed to add %s as file log' % logdestination)

        #setting the format
        if loglevel is 'debug':
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
        else:
            formatter = logging.Formatter("%(message)s")

        loginstace.logdic[logname]['handler'].setFormatter(formatter)


        if loglevel is 'info':
            loginstace.logdic[logname]['handler'].setLevel(logging.INFO)
        elif loglevel is 'quiet':
            loginstace.logdic[logname]['handler'].setLevel(logging.WARNING)
        elif loglevel is 'debug':
            loginstace.logdic[logname]['handler'].setLevel(logging.DEBUG)

        loginstace.addHandler(loginstace.logdic[logname]['handler'])
        loginstace.info('Logger %s has been add' %logname)

    def remove(self, cmdobj, logname = ''):

        loginstace = self.chkinstance(cmdobj)
        if not hasattr(loginstace,'logdic'):
            raise Exception('Logdic not found')

        if len(loginstace.logdic) is 0:
            print ('Error : Log list is zero')

        elif logname in loginstace.logdic:

            loginstace.removeHandler(loginstace.logdic[logname]['handler'])
            del loginstace.logdic[logname]
            print ('Logger %s has been removed' % logname)

        elif logname is 'all':
            loginstace.handlers = []
            loginstace.logdic = {}
            print 'All logger has been romoved'

        else:
            raise Exception('Failed to find %s in the loglist' % logname)

    def info(self, cmdobj,  logname = ''):

        loginstace = self.chkinstance(cmdobj)

        if not hasattr(loginstace,'logdic'):
            raise Exception('Logdic not found')

        print'{0:20s} {1:20s} {2:20s}'.format('Name','Destination','Level')
        def printlogdata(logname):
            print ('{0:20s} {1:20s} {2:20s}'.format(logname, loginstace.logdic[logname]['destination'],loginstace.logdic[logname]['level']))

        if logname != '' and logname in loginstace.logdic:
            printlogdata(loginstace.logdic[logname])
        else:
            for loghandler in loginstace.logdic:
                printlogdata(loghandler)

    def default(self, cmdobj):

        print 'Restoring logging system to default'

        if not hasattr(cmdobj, 'logger'):
            cmdobj.logger = logging.getLogger()

        self.remove(cmdobj, 'all')
        self.add(cmdobj, logname = 'default')
        self.add(cmdobj, logname = 'default_logfile' , loglevel = 'debug' , logdestination = 'crab.log')

        print 'Logger has been restored to default'


