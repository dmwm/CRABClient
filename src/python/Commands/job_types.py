from Commands import CommandResult
from Commands.SubCommand import SubCommand

class job_types(SubCommand):
    """
    List all the job types the client supports
    """

    ## name should become automatically generated
    name  = "job_type"
    visible = False


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        pass


    def __call__(self):
        ## need to add here something intelligent
        return CommandResult(0, {'job_types': ['cmssw']})
