"""
Contains the logic and wraps calls to WMCore.Credential.Proxy
"""

from WMCore.Credential.Proxy import Proxy

class CredentialInteractions(object):
    '''
    CredentialInteraction

    Takes care of wrapping Proxy interaction and defining common behaviour
    for all the client commands.
    '''

    def __init__(self, serverdn, myproxy, role, logger):
        '''
        Constructor
        '''
        self.logger = logger
        self.defaultDelegation = {
                                  'logger':          self.logger,
                                  'vo':              'cms',
                                  'myProxySvr':      myproxy,
                                  'proxyValidity'  : '24:00',
                                  'myproxyValidity': '7',
                                  'serverDN' :       serverdn,
                                  #'group' :          group,
                                  'role':            role
                                  }


    def createNewVomsProxy(self, timeleftthreshold = 0):
        """
        Handles the proxy creation:
           - checks if a valid proxy still exists
           - performs the creation if it is expired
        """
        ## TODO add the change to have user-cert/key defined in the config.
        userproxy = Proxy( self.defaultDelegation )

        proxytimeleft = 0
        try:
            self.logger.debug("Getting proxy life time left")
            # does it return an integer that indicates?
            proxytimeleft = userproxy.getTimeLeft()
            self.logger.debug("Proxy is valid: %i" % proxytimeleft)
        except Exception, ex:
            msg = "Problem checking voms proxy life time: %s " % str(ex)
            self.logger.error( msg )

        if proxytimeleft < timeleftthreshold:
            # creating the proxy
            self.logger.debug("Creating a proxy for %s hours" % self.defaultDelegation['proxyValidity'] )
            userproxy.create()
            self.logger.debug("Proxy created.")

        return userproxy.getSubject( )

    def createNewMyProxy(self, timeleftthreshold = 0):
        """
        Handles the MyProxy creation
        """

        myproxy = Proxy ( self.defaultDelegation )

        myproxytimeleft = 0
        try:
            self.logger.debug("Getting myproxy life time left for %s" % self.defaultDelegation["myProxySvr"])
            # does it return an integer that indicates?
            myproxytimeleft = myproxy.getMyProxyTimeLeft( serverRenewer = True )
            self.logger.debug("Myproxy is valid: %i" % myproxytimeleft)
        except Exception, ex:
            msg = "Problem checking myproxy life time: %s " % str(ex)
            self.logger.error( msg )

        if myproxytimeleft < timeleftthreshold:
            # creating the proxy
            self.logger.debug("Delegating a myproxy for %s days" % self.defaultDelegation['myproxyValidity'] )
            myproxy.delegate( serverRenewer = True )
            self.logger.debug("My-proxy delegated.")


