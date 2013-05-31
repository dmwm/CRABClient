"""
Contains the logic and wraps calls to WMCore.Credential.Proxy
"""
import logging

from WMCore.Credential.Proxy import Proxy

from CRABClient.client_exceptions import ProxyCreationException


class CredentialInteractions(object):
    '''
    CredentialInteraction

    Takes care of wrapping Proxy interaction and defining common behaviour
    for all the client commands.
    '''

    def __init__(self, serverdn, myproxy, role, group, logger):
        '''
        Constructor
        '''
        self.logger = logger
        self.defaultDelegation = {
                                  #do not print messages coming from Proxy library, but put them into the logfile
                                  'logger':          logging.getLogger('CRAB3:traceback'),
                                  'vo':              'cms',
                                  'myProxySvr':      myproxy,
                                  'proxyValidity'  : '24:00',
                                  'myproxyValidity': "720:00", #30 days
                                  'serverDN' :       serverdn,
                                  'group' :          group,
                                  'role':            role if role != '' else 'NULL'
                                  }
        self.proxyChanged = False


    def createNewVomsProxy(self, timeleftthreshold=0):
        """
        Handles the proxy creation:
           - checks if a valid proxy still exists
           - performs the creation if it is expired
        """
        ## TODO add the change to have user-cert/key defined in the config.
        userproxy = Proxy( self.defaultDelegation )
        userproxy.userDN = userproxy.getSubject()

        proxytimeleft = 0
        self.logger.debug("Getting proxy life time left")
        # does it return an integer that indicates?
        proxytimeleft = userproxy.getTimeLeft()
        self.logger.debug("Proxy is valid: %i" % proxytimeleft)

        #if it is not expired I check if role and/or group are changed
        if not proxytimeleft < timeleftthreshold and self.defaultDelegation['role']!=None and  self.defaultDelegation['group']!=None:
            group , role = userproxy.getUserGroupAndRoleFromProxy( userproxy.getProxyFilename())
            if group != self.defaultDelegation['group'] or role != self.defaultDelegation['role']:
                self.proxyChanged = True

        #if the proxy is expired, or we changed role and/or group, we need to create a new one
        if proxytimeleft < timeleftthreshold or self.proxyChanged:
            # creating the proxy
            self.logger.debug("Creating a proxy for %s hours" % self.defaultDelegation['proxyValidity'] )
            userproxy.create()
            proxytimeleft = userproxy.getTimeLeft()
            group , role = userproxy.getUserGroupAndRoleFromProxy( userproxy.getProxyFilename())

            if proxytimeleft > 0 and group == self.defaultDelegation['group'] and role == self.defaultDelegation['role']:
                self.logger.debug("Proxy created.")
            else:
                raise ProxyCreationException("Problems creating proxy.")

        return userproxy.getSubject( ), userproxy.getProxyFilename()

    def createNewMyProxy(self, timeleftthreshold=0, nokey=False):
        """
        Handles the MyProxy creation
        """
        myproxy = Proxy ( self.defaultDelegation )
        myproxy.userDN = myproxy.getSubject()

        myproxytimeleft = 0
        self.logger.debug("Getting myproxy life time left for %s" % self.defaultDelegation["myProxySvr"])
        # does it return an integer that indicates?
        myproxytimeleft = myproxy.getMyProxyTimeLeft(serverRenewer=True, nokey=nokey)
        self.logger.debug("Myproxy is valid: %i" % myproxytimeleft)

        if myproxytimeleft < timeleftthreshold or self.proxyChanged:
            # creating the proxy
            self.logger.debug("Delegating a myproxy for %s hours" % self.defaultDelegation['myproxyValidity'] )
            try:
                myproxy.delegate(serverRenewer = True, nokey=nokey)
                self.logger.debug("My-proxy delegated.")
            except Exception, ex:
                raise ProxyCreationException("Problems delegating My-proxy. Problem %s"%ex)

