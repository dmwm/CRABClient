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

    def __init__(self, serverdn, logger):
        '''
        Constructor
        '''
        self.logger = logger
        self.defaultDelegation = {
                                  'logger':    self.logger,
                                  'vo':        'cms',
                                  'myProxySvr':'myproxy.cern.ch',
                                  'proxyValidity'  : '24:00',
                                  'myproxyValidity': '7',
                                  'serverDN' :  serverdn
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
            try:
                self.logger.debug("Creating a proxy long %s" % self.defaultDelegation['proxyValidity'] )
                userproxy.create()
                self.logger.debug("Proxy created.")
            ## eventually it raises a generic exception
            except Exception, ex:
                msg = "Problem when creating a voms proxy: %s " % str(ex)
                self.logger.error( msg )
                raise Exception( msg )

        return userproxy.getSubject( )

    def createNewMyProxy(self, timeleftthreshold = 0):
        """
        Handles the MyProxy creation
        """

        myproxy = Proxy ( self.defaultDelegation )

        myproxytimeleft = 0
        try:
            self.logger.debug("Getting my-proxy life time left for %s" % self.defaultDelegation["myProxySvr"])
            # does it return an integer that indicates?
            myproxytimeleft = myproxy.getMyProxyTimeLeft( serverRenewer = True )
            self.logger.debug("My-proxy is valid: %i" % myproxytimeleft)
        except Exception, ex:
            msg = "Problem checking my-proxy life time: %s " % str(ex)
            self.logger.error( msg )

        if myproxytimeleft < timeleftthreshold:
            # creating the proxy
            try:
                self.logger.debug("Delegating a my-proxy long %s" % self.defaultDelegation['myproxyValidity'] )
                myproxy.delegate( serverRenewer = True )
                self.logger.debug("My-proxy delegated.")
            ## eventually it raises a generic exception
            except Exception, ex:
                msg = "Problem when creating a voms proxy: %s " % str(ex)
                self.logger.error( msg )
                raise Exception( msg )


