from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import getUsername


class checkusername(SubCommand):
    """
    Use to check extraction of username from DN
    """

    name = 'checkusername'

    def __call__(self):
        username = getUsername(self.proxy.proxyInfo, logger=self.logger)
        self.logger.info("Username is: %s", username)
        return username


    def terminate(self, exitcode):
        pass

