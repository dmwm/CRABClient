from Commands import CommandResult
from Commands.remote_copy import remote_copy
from client_utilities import loadCache
import json
import os

def getoutput(logger, configuration, server, options, requestname, requestarea):
    """
    """
    uri = '/crabinterface/crab/data'

    ## check input options and set destination directory
    if 'range' not in options or options['range'] is None:
        return CommandResult(1, 'Error: Range option is required')

    dest = os.path.join(requestarea, 'results')
    if 'outputpath' in options and options['outputpath'] is not None:
        dest = options['outputpath'][0]
    logger.debug("Setting the destination directory to %s " % dest )
    if not os.path.exists( dest ):
        logger.debug("Creating directory %s " % dest)
        os.makedirs( dest )
    elif not os.path.isdir( dest ):
        return CommandResult(1, 'Destination directory is a file')

    ## retrieving output files location from the server
    cachedinfo = loadCache(requestarea)
    logger.debug('Retrieving output for jobs %s in task %s' % ( options['range'], cachedinfo['RequestName'] ) )
    inputdict = {'range' : options['range'][0], 'requestID': cachedinfo['RequestName'] }
    dictresult, status, reason = server.get(uri, inputdict)

    logger.debug("Result: %s" % dictresult)

    if status != 200:
        msg = "Problem retrieving getoutput information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
        return CommandResult(1, msg)

    ## setting needed options for remote_copy command
    options['dest']      = dest
    options['inputdict'] = dictresult
    return remote_copy(logger, configuration, server, options, requestname, requestarea)

