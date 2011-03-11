"""
This is simply taking care of job submission
"""

from CredentialInteractions import CredentialInteractions
from clientcommands import CommandResult
from client_utilities import getJobTypes
import getpass
import json
import os
import cPickle

def submit(logger, configuration, server, options, requestname, requestarea):
    """
    Perform the submission to the CRABServer
    """
    logger.debug("Started submission")
    # If I'm submitting I need to deal with proxies
    proxy = CredentialInteractions(configuration.General.serverdn, logger)
    uniquerequestname = None

    defaultconfigreq = {"RequestType" : "Analysis"}

    userdefault = {
                   "Username" : getpass.getuser(),
                   "Group"    : "Analysis",
                   "Team"     : "Analysis",
                   "Email"    : "unknown",
                   "UserDN"   : "unknown"
                  }

    logger.info("Checking cerdentials")
    userdefault['UserDN'] = proxy.createNewVomsProxy( timeleftthreshold = 600 )
    logger.info("Registering user cerdentials")
    proxy.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * 3)

    logger.debug("Registering the user on the server")
    useruri = '/crab/user'
    result, status, reason = server.post( useruri, json.dumps( userdefault, sort_keys = False) )
    dictresult = json.loads(result)
    logger.debug("Result: %s" % dictresult)
    if status != 200:
        msg = "Problem registering user:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
        return CommandResult(1, msg)

    defaultconfigreq["Group"] = userdefault["Group"]
    defaultconfigreq["Team"]  = userdefault["Team"]
    defaultconfigreq["Requestor"] = userdefault['Username']
    defaultconfigreq["Username"]  = userdefault['Username']
    defaultconfigreq["RequestName"] = requestname

    ## create job types
    jobtypes = getJobTypes()
    if configuration.JobType.pluginName not in jobtypes:
        raise NameError("JobType %s not found or not supported." % configuration.JobType.pluginName)
    plugjobtype = jobtypes[configuration.JobType.pluginName](configuration, logger)
    inputfiles, jobconfig = plugjobtype.run()

    defaultconfigreq.update(jobconfig)

    ## TODO upload inputfiles
    logger.debug("Uploading inputfiles '%s' should be here" % str(inputfiles))

    uri = '/crab/task/' + defaultconfigreq["RequestName"]

    logger.info("Sending the request to the server")
    logger.debug("Submitting %s " % str( json.dumps( defaultconfigreq, sort_keys = False, indent = 4 ) ) )
    result, status, reason = server.post(uri, json.dumps( defaultconfigreq, sort_keys = False) )
    dictresult = json.loads(result)
    logger.debug("Result: %s" % dictresult)
    if status != 200:
        msg = "Problem sending the request:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
        return CommandResult(1, msg)
    elif dictresult.has_key("ID"):
        uniquerequestname = dictresult["ID"]
    else:
        msg = "Problem during submission, no request ID returned:\ninput:%s\noutput:%s\nreason:%s" \
               % (str(userdefault), str(dictresult), str(reason))
        return CommandResult(1, msg)

    touchfile  = open(os.path.join(requestarea, '.requestcache'), 'w')
    neededhandlers = {
                      "Server:" : server['conn'].host,
                      "Port:" : server['conn'].port,
                      "RequestName" : uniquerequestname
                     }
    cPickle.dump(neededhandlers, touchfile)
    touchfile.close()

    logger.info("Submission completed")
    logger.debug("Request ID: %s " % uniquerequestname)

    logger.debug("Ended submission")

    return CommandResult(0, None)
