"""
Abstract class that should be inherited by each job type plug-in
Conventions:
 1) the plug-in file name has to be equal to the plug-in class
 2) a plug-in needs to implement mainly the run method
"""

from WMCore.Configuration import Configuration
from FWCore.PythonUtilities.LumiList import LumiList

class BasicJobType(object):
    """
    BasicJobType

    TODO: thinking on having a job type help here...
    """

    def __init__(self, config, logger, workingdir):
        self.logger = logger
        ## Before everything checking if the config is ok
        if config:
            result, msg = self.validateConfig( config )
            if result:
                self.config  = config
                self.workdir = workingdir
            else:
                ## the config was not ok, returning a proper message
                raise Exception( msg )


    def run(self):
        """
        _run_

        Here goes the job type algorithm
        """
        raise NotImplementedError()


    def validateConfig(self):
        """
        _validateConfig_

        Allows to have a basic validation of the needed parameters
        """
        ## (boolean with the result of the validation, eventual error message)
        return (True, '')


    @staticmethod
    def mergeLumis(inputdata, lumimask):
        """
        Computes the processed lumis, merges if needed and returns the compacted list.
        """
        mergedlumis = LumiList()
        doublelumis = LumiList()
        for report in inputdata:
            doublelumis = mergedlumis & LumiList(runsAndLumis=report)
            mergedlumis = mergedlumis | LumiList(runsAndLumis=report)
            if doublelumis:
                self.logger.info("Warning: double run-lumis processed %s" % doublelumis)
        return mergedlumis.getCompactList(), (LumiList(compactList=lumimask) - mergedlumis).getCompactList()
