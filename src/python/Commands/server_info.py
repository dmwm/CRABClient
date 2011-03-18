from Commands import CommandResult

def server_info(logger, configuration, server, options, requestname, requestarea):
    """
    Get relevant information about the server
    """
    return CommandResult(0, {'myproxy': 'myproxy.cern.ch', 'server_dn': configuration.General.serverdn})
