"""
This is simply taking care of job submission
"""

from CredentialInteractions import CredentialInteractions
from Commands import CommandResult
from client_utilities import getJobTypes
import getpass
import json
import os
from client_utilities import createCache
from string import upper

def submit(logger, configuration, server, options, requestname, requestarea):
    """
    Perform the submission to the CRABServer
    """
    logger.debug("Started submission")
    # If I'm submitting I need to deal with proxies
    proxy = CredentialInteractions(
                                    configuration.General.serverdn,
                                    configuration.General.myproxy,
                                    getattr(configuration.User, "role", ""),
                                    logger
                                  )
    uniquerequestname = None

    defaultconfigreq = {"RequestType" : "Analysis"}

    userdefault = {
                   "Username" : getpass.getuser(),
                   "Group"    : getattr(configuration.User, "group", "Analysis"),
                   "Team"     : getattr(configuration.User, "team", "Analysis"),
                   "Email"    : configuration.User.email,
                   "UserDN"   : "unknown"
                  }

    logger.info("Checking credentials")
    userdefault['UserDN'] = proxy.createNewVomsProxy( timeleftthreshold = 600 )
    logger.info("Registering user credentials")
    proxy.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * 3)

    logger.debug("Registering the user on the server")
    useruri = '/crabinterface/crab/user'
    dictresult, status, reason = server.post( useruri, json.dumps( userdefault, sort_keys = False) )
    logger.debug("Result: %s" % str(dictresult))
    if status != 200:
        msg = "Problem registering user:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
        return CommandResult(1, msg)

    defaultconfigreq["Group"] = userdefault["Group"]
    defaultconfigreq["Team"]  = userdefault["Team"]
    defaultconfigreq["Requestor"]   = userdefault['Username']
    defaultconfigreq["Username"]    = userdefault['Username']
    defaultconfigreq["RequestName"] = requestname
    defaultconfigreq["RequestorDN"] = userdefault["UserDN"]

    ## create job types
    jobtypes = getJobTypes()
    if configuration.JobType.pluginName not in jobtypes:
        raise NameError("JobType %s not found or not supported." % configuration.JobType.pluginName)
    plugjobtype = jobtypes[upper(configuration.JobType.pluginName)](configuration, logger, requestarea)
    inputfiles, jobconfig = plugjobtype.run(defaultconfigreq)

    defaultconfigreq.update(jobconfig)

    ## TODO upload inputfiles
    logger.debug("Uploading inputfiles '%s' should be here" % str(inputfiles))
    #    self.configuration.General.sbservhost
    #    self.configuration.General.sbservport
    #    self.configuration.General.sbservtype
    #    self.configuration.General.sbservpath

    uri = '/crabinterface/crab/task/' + defaultconfigreq["RequestName"]

    logger.info("Sending the request to the server")
    logger.debug("Submitting %s " % str( json.dumps( defaultconfigreq, sort_keys = False, indent = 4 ) ) )
    dictresult, status, reason = server.post(uri, json.dumps( defaultconfigreq, sort_keys = False) )
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

    createCache( requestarea, server, uniquerequestname )

    logger.info("Submission completed")
    logger.debug("Request ID: %s " % uniquerequestname)

    logger.debug("Ended submission")

    return CommandResult(0, None)
