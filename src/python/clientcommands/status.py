from clientcommands import CommandResult

def status(logger, configuration, server, options, requestname, requestarea):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by -t/--task option
    """
    uri = '/crab/tasks/'
    if 'task' in options.keys():
        ## TODO
        for task in options['task']:
            logger.debug('Looking up detailed status of task %s' % task)
            server.get(uri + taskid)
    else:
        ## TODO
        logger.debug('Looking up task status overview')
        server.get(uri)
    return CommandResult(0, None)
