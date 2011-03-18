from Commands import CommandResult

def job_types(logger, configuration, server, options, requestname, requestarea):
    """
    List all the job types the client supports
    """
    return CommandResult(0, {'job_types': ['cmssw', 'storeresults']})
