from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import colors, getUserDNandUsername


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


    def standaloneCheck(self):
        return getUserDNandUsername(self.logger)


    def crabCheck(self):
        userdn = None
        username = None
        self.logger.info('Attempting to retrieve your username from SiteDB...')
        try:
            userdn = self.proxy.getUserDN()
            self.logger.info('Your DN is: %s' % userdn)
        except:
            self.logger.error('%Error%s: Unable to retrieve your DN from certificate.' % (colors.RED, colors.NORMAL))
            return {'DN': userdn, 'username': username}
        try:
            username = self.proxy.getUsernameFromSiteDB()
            self.logger.info('Your username is: %s' % username)
        except:
            self.logger.error('%Error%s: Unable to retrieve your username.' % (colors.RED, colors.NORMAL))
        return {'DN': userdn, 'username': username}


    def terminate(self, exitcode):
        pass

