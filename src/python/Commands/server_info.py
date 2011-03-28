from Commands import CommandResult

def server_info(logger, configuration, server, options, requestname, requestarea):
    """
    Get relevant information about the server
    """
    uri = '/crabinterface/crab/info'
    logger.debug('Looking up server information')
    dictresult, status, reason = server.get(uri)

    logger.debug("Result: %s" % dictresult)
    ## expecting something like
    # {u'AgentDN': '/C=IT/O=INFN/OU=Host/L=LNL/CN=crabas.lnl.infn.it', u'sandboxCacheType': 'gridFtp',
    #  u'basepath': '/data/CSstoragePath/', u'my_proxy': 'myproxy.cern.ch',
    #  u'SandBoxCacheEndpoint': 'crabas.lnl.infn.it', u'port': '2811'}

    if status != 200:
        msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
        return CommandResult(1, msg)

    return CommandResult(0, dictresult) #{'myproxy': 'myproxy.cern.ch', 'server_dn': configuration.General.serverdn})
