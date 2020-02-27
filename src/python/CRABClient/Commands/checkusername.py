from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import getUserDNandUsername


class checkusername(SubCommand):
    """
    Use to check extraction of username from DN
    """

    name = 'checkusername'

    def __call__(self):
        usernameDict = getUserDNandUsername(self.logger)
        return usernameDict


    def terminate(self, exitcode):
        pass

