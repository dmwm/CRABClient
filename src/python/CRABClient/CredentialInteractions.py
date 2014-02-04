"""
Contains the logic and wraps calls to WMCore.Credential.Proxy
"""
import logging

from WMCore.Credential.Proxy import Proxy, CredentialException

from CRABClient.client_exceptions import ProxyCreationException, EnvironmentException
from CRABClient.client_utilities import colors


class CredentialInteractions(object):
    '''
    CredentialInteraction

    Takes care of wrapping Proxy interaction and defining common behaviour
    for all the client commands.
    '''
    myproxyDesiredValidity = 30 #days

    def __init__(self, serverdn, myproxy, role, group, logger, myproxyAccount):
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
                                  'myproxyValidity': "%i:00" % (self.myproxyDesiredValidity*24), #30 days
                                  'serverDN' :       serverdn,
                                  'group' :          group,
                                  'role':            role if role != '' else 'NULL',
                                  'myproxyAccount' : myproxyAccount
                                  }
        self.proxyChanged = False

    def getUserName(self):
        """
        Return user name form DN
        """
        try:
            userproxy = Proxy( self.defaultDelegation )
        except CredentialException, ex:
            self.logger.debug(ex)
            raise EnvironmentException('Problem with Grid environment. %s ' %ex._message)
        return userproxy.getUserName()

    def createNewVomsProxy(self, timeleftthreshold=0):
        """
        Handles the proxy creation:
           - checks if a valid proxy still exists
           - performs the creation if it is expired
        """
        ## TODO add the change to have user-cert/key defined in the config.
        try:
            userproxy = Proxy( self.defaultDelegation )
        except CredentialException, ex:
            self.logger.debug(ex)
            raise EnvironmentException('Problem with Grid environment. %s ' %ex._message)
        userproxy.userDN = userproxy.getSubject()

        proxytimeleft = 0
        self.logger.debug("Getting proxy life time left")
        # returns an integer that indicates the number of seconds to the expiration of the proxy
        proxytimeleft = userproxy.getTimeLeft()
        self.logger.debug("Proxy is valid: %i" % proxytimeleft)

        #if it is not expired I check if role and/or group are changed
        if not proxytimeleft < timeleftthreshold and self.defaultDelegation['role']!=None and self.defaultDelegation['group']!=None:
            group, role = userproxy.getUserGroupAndRoleFromProxy( userproxy.getProxyFilename())
            if group != self.defaultDelegation['group'] or role != self.defaultDelegation['role']:
                #ask the user what he wants to do. Keep it or leave it?
                while True:
                    self.logger.info(("An existing proxy file has group=%s and role=%s, but in the configuration file you specified group=%s role=%s.\n" +
                                     "Do you want to overwrite it (Y/N)?") %
                                     (group, role, self.defaultDelegation['group'], self.defaultDelegation['role']))
                    res=raw_input()
                    if res in ['y','Y','n','N']: break
                #If he wants to overwrite the proxy then we do it, otherwise we exit asking to modify the config
                if res.upper() == 'Y':
                    self.proxyChanged = True
                else:
                    raise ProxyCreationException("Plase modify the config.User.voRole and config.User.voGroup parameters in your configuration file"+
                                                 "to match the existing proxy'")

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

        Let the following variables be

        timeleftthreshold: the proxy in myproxy should be delegated for at least this time (14 days)
        myproxytimeleft: current validity of your proxy in myproxy
        usercertDaysLeft: the number of days left before your user certificate expire
        myproxyDesiredValidity: delegate the proxy in myproxy for that time (30 days)

        If we need to renew the proxy in myproxy because its atributes has changed or because it is valid for
        less time than timeleftthreshold then we do it.

        Before doing that, we check when the user certificate is expiring. If it's within the timeleftthreshold (myproxytimeleft < timeleftthreshold)
        we delegate the proxy just for the time we need (checking first if we did not already do it since at some point
        usercertDaysLeft ~= myproxytimeleft and we don't need to delegate it at every command even though myproxytimeleft < timeleftthreshold).

        Note that a warning message is printed at every command it usercertDaysLeft < timeleftthreshold
        """
        myproxy = Proxy ( self.defaultDelegation )
        myproxy.userDN = myproxy.getSubject()

        myproxytimeleft = 0
        self.logger.debug("Getting myproxy life time left for %s" % self.defaultDelegation["myProxySvr"])
        # return an integer that indicates the number of seconds to the expiration of the proxy in myproxy
        myproxytimeleft = myproxy.getMyProxyTimeLeft(serverRenewer=True, nokey=nokey)
        self.logger.debug("Myproxy is valid: %i" % myproxytimeleft)

        if myproxytimeleft < timeleftthreshold or self.proxyChanged or myproxy.trustedRetrievers!=self.defaultDelegation['serverDN']:
            # checking the enddate of the user certificate
            usercertDaysLeft = myproxy.getUserCertEnddate()

            #if the certificate is going to expire print a warning. This is going to bre printed at every command if
            #the myproxytimeleft is inferior to the timeleftthreshold
            if usercertDaysLeft < self.myproxyDesiredValidity:
                self.logger.info("%sYour user certificate is going to expire in %s days. Please renew it! %s"\
                                 % (colors.RED, usercertDaysLeft, colors.NORMAL) )
                #check if usercertDaysLeft ~= myproxytimeleft which means we already delegated the proxy for as long as we could
                if abs(usercertDaysLeft*60*60*24 - myproxytimeleft) < 60*60*24: #less than one day between usercertDaysLeft and myproxytimeleft
                    return
                #adjust the myproxy delegation time accordingly to the user cert validity
                self.logger.info("%sDelegating your proxy for %s days instead of %s %s"\
                                 % (colors.RED, usercertDaysLeft, self.myproxyDesiredValidity, colors.NORMAL) )
                myproxy.myproxyValidity = "%i:00" % (usercertDaysLeft*24)

            # creating the proxy
            self.logger.debug("Delegating a myproxy for %s hours" % self.defaultDelegation['myproxyValidity'] )
            try:
                myproxy.delegate(serverRenewer = True, nokey=nokey)
                self.logger.debug("My-proxy delegated.")
            except Exception, ex:
                raise ProxyCreationException("Problems delegating My-proxy. %s"%ex._message)

