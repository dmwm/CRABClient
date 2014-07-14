import logging
import sys
import pycurl
from httplib import HTTPException
from CRABClient.ClientMapping import mapping
from CRABClient.ClientAPI.ClientLogger import ClientLogger
from WMCore.Configuration import Configuration

class Command():

    def __call__(self, command = None, *args, **kwargs):
        if command == None:
            raise Exception('No command is given')
        if len(args) == 0 and len(kwargs) == 0 :
            raise Exception('Argumnet must be given')

        #if the user provide a class config rather than a config python file
        if 'config' in kwargs.keys() and isinstance(kwargs['config'], Configuration):
            cmdarg = ['--config' , kwargs['config']]
            if len(args) != 0:
                cmdarg.extend(args)
        else:
            #all kwargs argument have to be transformed into a list, first white space is removed, then '--' and '=' is added to kwargs.keys and kwargs value respectively. Then joined
            cmdarg = [''.join(('--'+str(arg)+'='+str(kwargs[arg])).split()) for arg in kwargs]
            cmdarg.extend(args)

        try:
            mod = __import__('CRABClient.Commands.%s' % command, fromlist=command)
        except ImportError:
            raise Exception('Wrong crab command give, command give: %s' % command)
        try:
            cmdobj = getattr(mod, command)(self.logger , cmdarg)
            cmdobj.fromapi = True

            return cmdobj()

        except HTTPException, he:
            self.logger.info("Error contacting the server.")
            if he.status==503 and he.result.find("CMSWEB Error: Service unavailable")!=-1:
                self.logger.info("It seems the CMSWEB frontend is not responding. Please check: https://twiki.cern.ch/twiki/bin/viewauth/CMS/ScheduledInterventions")
            if he.headers.has_key('X-Error-Detail'):
                self.logger.info('Server answered with: %s' % he.headers['X-Error-Detail'])
            if he.headers.has_key('X-Error-Info'):
                reason = he.headers['X-Error-Info']
                for parname in mapping['submit']['map']:
                    for tmpmsg in ['\''+parname+'\' parameter','Parameter \''+parname+'\'']:
                        if tmpmsg in reason and mapping['submit']['map'][parname]['config']:
                            reason = reason.replace(tmpmsg,tmpmsg.replace(parname,mapping['submit']['map'][parname]['config']))
                            break
                    else:
                        continue
                    break
                self.logger.info('Reason is: %s' % reason)
            #The following goes to the logfile.
            errmsg = "ERROR: %s (%s): " % (he.reason, he.status)
            ## answer can be a json or not
            try:
                errmsg += " '%s'" % he.result
            except ValueError:
                pass
            self.logger.info(errmsg)
            self.logger.info('Command failed with URI: %s' % he.url)
            self.logger.info('     Input data: %s' % he.req_data)
            self.logger.info('     Request headers: %s' % he.headers)
            logging.getLogger('CRAB3:traceback').exception('Caught exception')

            raise HTTPException , he

        except pycurl.error, pe:
            self.logger.error(pe)
            logging.getLogger('CRAB3:traceback').exception('Caught exception')
            if pe[1].find('(DNS server returned answer with no data)'):
                self.logger.info("It seems the CMSWEB frontend is not responding. Please check: https://twiki.cern.ch/twiki/bin/viewauth/CMS/ScheduledInterventions")

            raise Exception

        except SystemExit , he:
            self.logger.info(he)

            raise Exception

    def __init__(self, loglevel = 'info'):

        #setting the enviroment log level to debug

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        log = ClientLogger()
        log.add(self, logname = 'default')
        log.add(self, logname = 'default_logfile' , loglevel = 'debug' , logdestination = 'crab.log')


