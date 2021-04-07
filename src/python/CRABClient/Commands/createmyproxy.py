from datetime import timedelta

from CRABClient.Commands.SubCommand import SubCommand

from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.ClientUtilities import server_info

class createmyproxy(SubCommand):
    """
    creates a new credential in myproxy valid for --days days
    if existing credential already lasts longer, it is not changed
    """

    name = 'createmyproxy'
    RENEW_MYPROXY_THRESHOLD = 15

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs, disable_interspersed_args=True)

        self.configreq = None
        self.configreq_encoded = None

    def __call__(self):

        proxy = CredentialInteractions('', '', '', '', self.logger)
        days = self.options.days
        proxy.setMyProxyValidity(int(days) * 24 * 60)  # minutes
        timeLeftThreshold = int(days) * 24 * 60 * 60 # seconds

        self.logger.info("Checking credentials")

        # need an X509 proxy in order to talk with CRABServer to get list of myproxy authorized retrievers
        proxy.proxyInfo = proxy.createNewVomsProxy(timeLeftThreshold=720)
        baseurl = '/crabserver/prod'
        alldns = server_info(crabserver=self.crabserver, subresource='delegatedn')
        for serverdn in alldns['services']:
            proxy.defaultDelegation['serverDN'] = serverdn
            proxy.defaultDelegation['myProxySvr'] = 'myproxy.cern.ch'

            self.logger.info("Registering user credentials for server %s" % serverdn)
            (credentialName, myproxyTimeleft) = proxy.createNewMyProxy(timeleftthreshold=timeLeftThreshold, nokey=True)
            self.logger.info("Credential exists on myproxy: username: %s  - validity: %s", credentialName,
                             str(timedelta(seconds=myproxyTimeleft)))
            (credentialName, myproxyTimeleft) = proxy.createNewMyProxy2(timeleftthreshold=timeLeftThreshold, nokey=True)
            self.logger.info("Credential exists on myproxy: username: %s  - validity: %s", credentialName,
                             str(timedelta(seconds=myproxyTimeleft)))

        return


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options.
        """

        self.parser.add_option('--days',
                               dest='days',
                               default=30,
                               type='int',
                               help="Set the validity (in days) for the credential. Default is 30.")

    #def terminate(self, exitcode):
    #    pass
