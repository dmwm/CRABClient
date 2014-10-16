import os
import string
import commands
import urllib

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_utilities import getUsername
from CRABClient.client_exceptions import UsernameException


class checkusername(SubCommand):
    """
    Use to check extraction of username from SiteDB
    """

    ## Let the user to test the extraction of his/her username from SiteDB.
    ## There are two methods for doing this: standaloneCheck() and crabCheck().
    ## It was decided that it is enough with the standaloneCheck() method,
    ## so we use only this option. I didn't remove the crabCheck() method
    ## just in case we realize in the future that this method is also useful.
    name = 'checkusername'

    def __call__(self):
        return self.standaloneCheck()


    def terminate(self, exitcode):
        pass


    def standaloneCheck(self):

        self.logger.info('Attempting to extract your username from SiteDB...')
        ## Direct stderr from voms-proxy-* to dev/null to avoid stupid Java messages :-(
        status, dn = commands.getstatusoutput('eval `scram unsetenv -sh`; voms-proxy-info -identity 2>/dev/null')
        if status:
            self.logger.info('WARNING: Unable to retrieve your DN.')
            return
        self.logger.info('Your DN is: %s' % dn)
        try:
            username = getUsername()
        except UsernameException:
            self.logger.info("SiteDB returned no username for the above DN")
        else:
            self.logger.info("Your username is: %s" % username)
        self.logger.info('Finished')

        return {'DN': dn, 'username': username}

    def crabCheck(self):
        self.logger.info('Attempting to extract your username from SiteDB...')
        try:
            userdn = self.proxy.getUserDN()
            self.logger.info('Your DN is: %s' % userdn)
        except:
            self.logger.info('WARNING: Unable to retrieve your DN.')
        else:
            try:
                username = self.proxy.getHyperNewsName()
                self.logger.info('Your username is: %s' % username)
            except:
                self.logger.info('WARNING: Unable to retrieve your username.')
        self.logger.info('Finished')

        return {'DN': userdn, 'username': username}
