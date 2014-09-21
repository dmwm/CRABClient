"""
Contains the logic and wraps calls to WMCore.Credential.Proxy
"""
import logging
import os

from WMCore.Credential.Proxy import Proxy, CredentialException
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from CRABClient.client_exceptions import ProxyCreationException, EnvironmentException
from CRABClient.client_utilities import colors


class CredentialInteractions(object):
    '''
    CredentialInteraction

    Takes care of wrapping Proxy interaction and defining common behaviour
    for all the client commands.
    '''
    myproxyDesiredValidity = 30 ## days

    def __init__(self, serverdn, myproxy, role, group, logger, myproxyAccount):
        '''
        Constructor
        '''
        self.logger = logger
        self.defaultDelegation = {
                                  #do not print messages coming from Proxy library, but put them into the logfile
                                  'logger':          logging.getLogger('CRAB3'),
                                  'vo':              'cms',
                                  'myProxySvr':      myproxy,
                                  'proxyValidity'  : '24:00', ## hh:mm
                                  'myproxyValidity': '%i:00' % (self.myproxyDesiredValidity*24), ## hh:mm
                                  'serverDN' :       serverdn,
                                  'group' :          group,
                                  'role':            role if role != '' else 'NULL',
                                  'myproxyAccount' : myproxyAccount
                                  }
        self.proxyChanged = False
        self.certLocation = '~/.globus/usercert.pem' if 'X509_USER_CERT' not in os.environ else os.environ['X509_USER_CERT']


    def setVOGroupVORole(self, group, role):
        self.defaultDelegation['group'] = group
        self.defaultDelegation['role'] = role if role != '' else 'NULL'


    def setMyProxyAccount(self, myproxy_account):
        self.defaultDelegation['myproxyAccount'] = myproxy_account


    def setProxyValidity(self, validity):
        self.defaultDelegation['proxyValidity'] = '%i:%02d' % (int(validity/60), int(validity%60))


    def setMyProxyValidity(self, validity):
        self.defaultDelegation['myproxyValidity'] = '%i:%02d' % (int(validity/60), int(validity%60))


    def setServerDN(self, serverdn):
        self.defaultDelegation['serverDN'] = serverdn


    def setMyProxyServer(self, server):
        self.defaultDelegation['myProxySvr'] = server


    def proxy(self):
        try:
            proxy = Proxy(self.defaultDelegation)
        except CredentialException, ex:
            self.logger.debug(ex)
            raise EnvironmentException('Problem with Grid environment: %s ' % ex._message)
        return proxy


    def getUserDN(self):
        proxy = self.proxy()
        return proxy.getSubjectFromCert(self.certLocation)


    def getHyperNewsName(self):
        """
        Return a the client hypernews name
        """
        proxy = self.proxy()
        userdn = proxy.getSubjectFromCert(self.certLocation)
        sitedb = SiteDBJSON({"key": proxy.getProxyFilename(), "cert": proxy.getProxyFilename()})
        return sitedb.dnUserName(userdn)


    def getUserName(self):
        """
        Return user name form DN
        """
        proxy = self.proxy()
        return proxy.getUserName()


    def getFilename(self):
        proxy = self.proxy()
        proxy_file_name = proxy.getProxyFilename()
        if not os.path.isfile(proxy_file_name):
            self.logger.debug("Proxy file %s not found" % proxy_file_name)
            return ''
        return proxy_file_name


    def getTimeLeft(self):
        proxy = self.proxy()
        return proxy.getTimeLeft() ## Returns an integer that indicates the number of seconds to the expiration of the proxy


    def createNewVomsProxySimple(self, time_left_threshold = 0):
        proxy_created = False
        proxy = self.proxy()
        self.logger.debug("Checking credentials")
        proxy_file_name = proxy.getProxyFilename()
        if not os.path.isfile(proxy_file_name):
            self.logger.debug("Proxy file %s not found" % proxy_file_name)
            proxy_time_left = 0
        else:
            self.logger.debug("Found proxy file %s" % proxy_file_name)
            self.logger.debug("Getting proxy life time left")
            proxy_time_left = proxy.getTimeLeft()
            hours, minutes, seconds = int(proxy_time_left/3600), int((proxy_time_left%3600)/60), int((proxy_time_left%3600)%60)
            self.logger.debug("Proxy valid for %02d:%02d:%02d hours" % (hours, minutes, seconds))
        if proxy_time_left < time_left_threshold:
            msg = "Creating proxy for %s hours with VO role '%s' and VO group '%s'" \
                  % (self.defaultDelegation['proxyValidity'], self.defaultDelegation['role'], self.defaultDelegation['group'])
            self.logger.debug(msg)
            proxy.create()
            proxy_time_left = proxy.getTimeLeft()
            group, role = proxy.getUserGroupAndRoleFromProxy(proxy.getProxyFilename())
            if proxy_time_left > 0 and group == self.defaultDelegation['group'] and role == self.defaultDelegation['role']:
                self.logger.debug("Proxy created")
                proxy_created = True
            else:
                raise ProxyCreationException("Problems creating proxy")
           
        return proxy_created


    def createNewVomsProxy(self, time_left_threshold = 0, proxy_created_by_crab = False):
        """
        Handles the proxy creation:
           - checks if a valid proxy still exists
           - performs the creation if it is expired
        """
        ## TODO add the change to have user-cert/key defined in the config.
        proxy = self.proxy()
        proxy.userDN = proxy.getSubjectFromCert(self.certLocation)
        self.logger.debug("Checking credentials")
        proxy_file_name = proxy.getProxyFilename()
        if not os.path.isfile(proxy_file_name):
            self.logger.debug("Proxy file %s not found" % proxy_file_name)
            proxy_time_left = 0
        else:
            self.logger.debug("Found proxy file %s" % proxy_file_name)
            self.logger.debug("Getting proxy life time left")
            proxy_time_left = proxy.getTimeLeft()
            hours, minutes, seconds = int(proxy_time_left/3600), int((proxy_time_left%3600)/60), int((proxy_time_left%3600)%60)
            self.logger.debug("Proxy valid for %02d:%02d:%02d hours" % (hours, minutes, seconds))

        ## If the proxy is not expired, we check if role and/or group are changed.
        if proxy_time_left > time_left_threshold and self.defaultDelegation['role'] != None and self.defaultDelegation['group'] != None and not proxy_created_by_crab:
            group, role = proxy.getUserGroupAndRoleFromProxy(proxy_file_name)
            if group != self.defaultDelegation['group'] or role != self.defaultDelegation['role']:
                ## Ask the user what he wants to do. Keep it or leave it?
                while True:
                    self.logger.info(("Proxy file %s exists with VO group = '%s' and VO role = '%s', but you have specified to use VO group = '%s' and VO role = '%s'." +
                                      "\nDo you want to overwrite the proxy (Y/N)?") \
                                      % (proxy_file_name, group, role, self.defaultDelegation['group'], self.defaultDelegation['role']))
                    res=raw_input()
                    if res in ['y','Y','n','N']: break
                ## If he wants to overwrite the proxy then we do it, otherwise we exit asking to modify the config.
                if res.upper() == 'Y':
                    self.proxyChanged = True
                else:
                    raise ProxyCreationException("Please modify the User.voRole and User.voGroup parameters in your configuration file to match the existing proxy")

        ## If the proxy is expired, or we changed role and/or group, we need to create a new proxy.
        if proxy_time_left < time_left_threshold or self.proxyChanged:
            ## Create the proxy.
            self.logger.debug("Creating new proxy for %s hours" % self.defaultDelegation['proxyValidity'] )
            proxy.create()
            proxy_time_left = proxy.getTimeLeft()
            group, role = proxy.getUserGroupAndRoleFromProxy(proxy.getProxyFilename())
            if proxy_time_left > 0 and group == self.defaultDelegation['group'] and role == self.defaultDelegation['role']:
                self.logger.debug("Proxy created.")
            else:
                raise ProxyCreationException("Problems creating proxy.")

        return proxy.userDN, proxy.getProxyFilename()


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
        myproxy.userDN = myproxy.getSubjectFromCert(self.certLocation)

        myproxytimeleft = 0
        self.logger.debug("Getting myproxy life time left for %s" % self.defaultDelegation["myProxySvr"])
        # return an integer that indicates the number of seconds to the expiration of the proxy in myproxy
        myproxytimeleft = myproxy.getMyProxyTimeLeft(serverRenewer=True, nokey=nokey)
        self.logger.debug("Myproxy is valid: %i" % myproxytimeleft)

        trustRetrListChanged = myproxy.trustedRetrievers!=self.defaultDelegation['serverDN'] #list on the REST and on myproxy are different
        if myproxytimeleft < timeleftthreshold or self.proxyChanged or trustRetrListChanged:
            # checking the enddate of the user certificate
            usercertDaysLeft = myproxy.getUserCertEnddate()
            if usercertDaysLeft == 0:
                msg = "%sYOUR USER CERTIFICATE IS EXPIRED (OR WILL EXPIRE TODAY). YOU CANNOT USE THE CRAB3 CLIENT. PLEASE REQUEST A NEW CERTIFICATE HERE https://gridca.cern.ch/gridca/ AND SEE https://ca.cern.ch/ca/Help/?kbid=024010%s"\
                                        % (colors.RED, colors.NORMAL)
                raise ProxyCreationException(msg)

            #if the certificate is going to expire print a warning. This is going to bre printed at every command if
            #the myproxytimeleft is inferior to the timeleftthreshold
            if usercertDaysLeft < self.myproxyDesiredValidity:
                self.logger.info("%sYour user certificate is going to expire in %s days. Please request a new certificate here https://gridca.cern.ch/gridca/ and see https://ca.cern.ch/ca/Help/?kbid=024010 %s"\
                                 % (colors.RED, usercertDaysLeft, colors.NORMAL) )
                #check if usercertDaysLeft ~= myproxytimeleft which means we already delegated the proxy for as long as we could
                if abs(usercertDaysLeft*60*60*24 - myproxytimeleft) < 60*60*24 and not trustRetrListChanged: #less than one day between usercertDaysLeft and myproxytimeleft
                    return
                #adjust the myproxy delegation time accordingly to the user cert validity
                self.logger.info("%sDelegating your proxy for %s days instead of %s %s"\
                                 % (colors.RED, usercertDaysLeft, self.myproxyDesiredValidity, colors.NORMAL) )
                myproxy.myproxyValidity = "%i:00" % (usercertDaysLeft*24)

            # creating the proxy
            self.logger.debug("Delegating a myproxy for %s hours" % myproxy.myproxyValidity )
            try:
                myproxy.delegate(serverRenewer = True, nokey=nokey)
                myproxytimeleft = myproxy.getMyProxyTimeLeft(serverRenewer=True, nokey=nokey)
                if myproxytimeleft <= 0:
                    raise ProxyCreationException("It seems your proxy has not been delegated to myproxy. Please check the logfile for the exact error "+\
                                                            "(it might simply you typed a wrong password)")
                else:
                    self.logger.debug("My-proxy delegated.")
            except Exception, ex:
                msg = ex._message if hasattr(ex, '_message') else str(ex)
                raise ProxyCreationException("Problems delegating My-proxy. %s" % msg)

