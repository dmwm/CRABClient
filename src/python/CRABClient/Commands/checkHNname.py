import os
import string
import commands
import urllib

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_utilities import getHyperNewsName
from CRABClient.client_exceptions import HyperNewsNameException


class checkHNname(SubCommand):
    """
    Use to check user Hypernews / Cern username from SiteDB
    """
    #Let the user to test the extraction of his/her HN username from SiteDB.
    #There are two methods for doing this: standaloneCheck() and crabCheck().
    #It was decided that it is enough with the standaloneCheck() method,
    #so we use only this option. I didn't remove the crabCheck() method
    #just in case we realize in the future that this method is also useful.

    name = 'checkHNname'

    def __call__(self):
        return self.standaloneCheck()


    def terminate(self, exitcode):
        pass


    def standaloneCheck(self):

        self.logger.info('Attempting to extract your CMS HyperNews username from SiteDB...')
        ## Direct stderr from voms-proxy-* to dev/null to avoid stupid Java messages :-(
        status, dn = commands.getstatusoutput('eval `scram unsetenv -sh`; voms-proxy-info -identity 2>/dev/null')
        if status:
            self.logger.info('WARNING: Unable to retrieve your DN.')
            return
        self.logger.info('Your DN is: %s' % dn)
        try:
            hn_username = getHyperNewsName()
        except HyperNewsNameException:
            self.logger.info("SiteDB returned no username for the above DN")
        else:
            self.logger.info("Your CMS HyperNews username is: %s" % hn_username)
        self.logger.info('Finished')

        return {'DN' : dn , 'HNusername' : hn_username}

    def crabCheck(self):
        self.logger.info('Attempting to extract your HN username from SiteDB...')
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

        return {'DN' : userdn , 'HNusername' : hn_username}
