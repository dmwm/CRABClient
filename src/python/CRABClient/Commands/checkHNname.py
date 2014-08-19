import os
import string
import commands
import urllib

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import MissingOptionException
from CRABClient.CredentialInteractions import CredentialInteractions


class checkHNname(SubCommand):
    """
    Let the user to test the extraction of his/her HN username from SiteDB.
    There are two methods for doing this: standaloneCheck() and crabCheck().
    It was decided that it is enough with the standaloneCheck() method,
    so we use only this option. I didn't remove the crabCheck() method
    just in case we realize in the future that this method is also useful.
    """
    name = 'checkHNname'

    def __call__(self):

        self.standaloneCheck()


    def terminate(self, exitcode):
        pass


    def standaloneCheck(self):

        self.logger.info('Starting check in standalone mode...')
        ## Direct stderr from voms-proxy-* to dev/null to avoid stupid Java messages :-(
        status, dn = commands.getstatusoutput('eval `scram unsetenv -sh`; voms-proxy-info -identity 2>/dev/null')
        if status == 0:
           self.logger.info('Your DN is: %s' % dn)
        else:
           self.logger.info('WARNING: Unable to retrieve your DN.')
        status, proxy_file = commands.getstatusoutput('eval `scram unsetenv -sh`; voms-proxy-info -path 2>/dev/null')
        if status != 0:
            self.logger.info('ERROR getting proxy path')
        os.environ['X509_USER_PROXY'] = proxy_file
        if not 'X509_CERT_DIR' in os.environ:
            os.environ['X509_CERT_DIR'] = '/etc/grid-security/certificates'
        cmd  = "curl -s --capath $X509_CERT_DIR --cert $X509_USER_PROXY --key $X509_USER_PROXY 'https://cmsweb.cern.ch/sitedb/data/prod/whoami'"
        cmd += " | tr ':,' '\n'"
        cmd += " | grep -A1 login"
        cmd += " | tail -1"
        status, hn_username = commands.getstatusoutput(cmd)
        hn_username = string.strip(hn_username) 
        hn_username = hn_username.replace('"','')
        self.logger.info('Your HN username is: %s' % hn_username)
        self.logger.info('Finished')


    def crabCheck(self):
    
        self.logger.info('Starting check in CRAB-like mode...')
        try:
            userdn = self.proxy.getUserDN()
            self.logger.info('Your DN is: %s' % userdn)
        except:
            self.logger.info('WARNING: Unable to retrieve your DN.')
        else:
            try:
                hn_username = self.proxy.getHyperNewsName()
                self.logger.info('Your HN username is: %s' % hn_username)
            except:
                self.logger.info('WARNING: Unable to retrieve your HN username.')
        self.logger.info('Finished')

