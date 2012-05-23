"""
PrivateMC job type plug-in
"""

from CRABClient.JobType.Analysis import Analysis


class PrivateMC(Analysis):
    """
    PrivateMC job type plug-in
    """

    def run(self, requestConfig):
        """
        Override run() for JobType
        """
        tarFilename, configArguments = super(PrivateMC, self).run(requestConfig)
        configArguments['jobtype'] = 'PrivateMC'
        return tarFilename, configArguments

    def validateConfig(self, config):
        """
        Validate the PrivateMC portion of the config file making sure
        required values are there and optional values don't conflict. Subclass to CMSSW for most of the work
        """

        valid, reason = self.validateBasicConfig(config)

        # Put MC requirements here

        return (valid, reason)
