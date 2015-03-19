"""
Contains the logic and wraps calls to WMCore.Credential.Proxy
"""
import logging
import os

from WMCore.Credential.Proxy import Proxy, CredentialException
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from CRABClient.ClientExceptions import ProxyCreationException, EnvironmentException
from CRABClient.ClientUtilities import colors


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


    def setMyProxyAccount(self, myproxyAccount):
        self.defaultDelegation['myproxyAccount'] = myproxyAccount


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
        userdn = proxy.getSubjectFromCert(self.certLocation)
        return userdn


    def getUsername(self):
        proxy = self.proxy()
        username = proxy.getUsername()
        return username


    def getUsernameFromSiteDB(self):
        """
        Retrieve the user's username as it appears in SiteDB.
        """
        proxy = self.proxy()
        userdn = proxy.getSubjectFromCert(self.certLocation)
        sitedb = SiteDBJSON({"key": proxy.getProxyFilename(), "cert": proxy.getProxyFilename()})
        username = sitedb.dnUserName(userdn)
        return username


    def getUserName(self):
        """
        Retrieve the user's name from the DN in the proxy.
        """
        proxy = self.proxy()
        return proxy.getUserName()


    def getFilename(self):
        proxy = self.proxy()
        proxyfilename = proxy.getProxyFilename()
        if not os.path.isfile(proxyfilename):
            self.logger.debug("Proxy file %s not found" % proxyfilename)
            return ''
        return proxyfilename


    def getTimeLeft(self):
        proxy = self.proxy()
        return proxy.getTimeLeft() ## Returns an integer that indicates the number of seconds to the expiration of the proxy


    def createNewVomsProxySimple(self, timeleftthreshold = 0):
        proxycreated = False
        proxy = self.proxy()
        self.logger.debug("Checking credentials")
        proxyfilename = proxy.getProxyFilename()
        if not os.path.isfile(proxyfilename):
            self.logger.debug("Proxy file %s not found" % (proxyfilename))
            proxytimeleft = 0
        else:
            self.logger.debug("Found proxy file %s" % (proxyfilename))
            self.logger.debug("Getting proxy life time left")
            proxytimeleft = proxy.getTimeLeft()
            hours, minutes, seconds = int(proxytimeleft/3600), int((proxytimeleft%3600)/60), int((proxytimeleft%3600)%60)
            self.logger.debug("Proxy valid for %02d:%02d:%02d hours" % (hours, minutes, seconds))
        ## Create a new proxy if the current one is expired or if we were instructed
        ## to change the proxy for a new one.
        if proxytimeleft < timeleftthreshold:
            msg  = "Creating new proxy for %s hours" % (self.defaultDelegation['proxyValidity'])
            msg += " with VO group '%s' and VO role '%s'." % (self.defaultDelegation['group'], self.defaultDelegation['role'])
            self.logger.debug(msg)
            ## Create the proxy.
            proxy.create()
            ## Check that the created proxy has the expected VO group and role (what
            ## we have set in the defaultDelegation dictionary).
            proxytimeleft = proxy.getTimeLeft()
            group, role = proxy.getUserGroupAndRoleFromProxy(proxy.getProxyFilename())
            if proxytimeleft > 0 and group == self.defaultDelegation['group'] and role == self.defaultDelegation['role']:
                self.logger.debug("Proxy created.")
                proxycreated = True
            else:
                raise ProxyCreationException("Problems creating proxy.")
           
        return proxycreated


    def createNewVomsProxy(self, timeleftthreshold = 0, proxyCreatedByCRAB = False, \
                           proxyOptsSetPlace = None):
        """
        Handles the proxy creation:
           - checks if a valid proxy still exists
           - performs the creation if it is expired
        """
        ## TODO add the change to have user-cert/key defined in the config.
        proxy = self.proxy()
        proxy.userDN = proxy.getSubjectFromCert(self.certLocation)
        self.logger.debug("Checking credentials")
        proxyfilename = proxy.getProxyFilename()
        if not os.path.isfile(proxyfilename):
            self.logger.debug("Proxy file %s not found" % (proxyfilename))
            proxytimeleft = 0
        else:
            self.logger.debug("Found proxy file %s" % (proxyfilename))
            self.logger.debug("Getting proxy life time left")
            proxytimeleft = proxy.getTimeLeft()
            hours, minutes, seconds = int(proxytimeleft/3600), int((proxytimeleft%3600)/60), int((proxytimeleft%3600)%60)
            self.logger.debug("Proxy valid for %02d:%02d:%02d hours" % (hours, minutes, seconds))

        ## Make sure proxyOptsSetPlace is a dictionary and has the expected keys.
        if proxyOptsSetPlace is None or type(proxyOptsSetPlace) != dict:
            proxyOptsSetPlace = {}
        if 'role' not in proxyOptsSetPlace:
            proxyOptsSetPlace['role'] = ''
        if 'group' not in proxyOptsSetPlace:
            proxyOptsSetPlace['group'] = ''

        ## Get the VO group and role in the proxy.
        group, role = proxy.getUserGroupAndRoleFromProxy(proxyfilename)
        ## If the VO group or role in the proxy are different than in the defaultDelegation
        ## dictionary, and the values in that dictionary were actually not set by the user
        ## (either via the CRAB configuration file or via the command line options) or
        ## copied from the request cache, then set the VO group and role in defaultDelegation
        ## to what is in the proxy. This also guarantees that in case we have to create a new
        ## proxy, we will create it with the correct group and role.
        if (role != self.defaultDelegation['role'] and not proxyOptsSetPlace['role']) or \
           (group != self.defaultDelegation['group'] and not proxyOptsSetPlace['group']):
            msgadd = {}
            if (group != self.defaultDelegation['group'] and not proxyOptsSetPlace['group']):
                self.defaultDelegation['group'] = group
                proxy.group = group
                msgadd['group'] = "'%s'" % (group)
            if (role != self.defaultDelegation['role'] and not proxyOptsSetPlace['role']):
                self.defaultDelegation['role'] = role
                proxy.role = role
                msgadd['role'] = "'%s'" % (role)
            msg  = "You haven't specify any particular VO %s." % (" and ".join(msgadd.keys()))
            msg += " Will use the VO %s from your proxy (i.e. %s)." % (" and ".join(msgadd.keys()), " and ".join(msgadd.values()))
            self.logger.debug(msg)

        ## If the proxy (not created by CRAB) is not expired, ...
        if (proxytimeleft > timeleftthreshold) and not proxyCreatedByCRAB and \
           (self.defaultDelegation['role'] != None and self.defaultDelegation['group'] != None):
            ## ... we check if the VO role and/or group in the proxy are the same as
            ## the ones in defaultDelegation (which were either specified by the user
            ## in the CRAB configuration file or in the command line options, or copied
            ## from the request cache).
            if (group != self.defaultDelegation['group'] and proxyOptsSetPlace['group']) or \
               (role  != self.defaultDelegation['role']  and proxyOptsSetPlace['role'] ):
                ## If they are not the same, force the user to either create a new proxy
                ## with the same group and role as in defaultDelegation, or to change the
                ## CRAB configuration file or the command line options.
                while True:
                    msgadd1 = []
                    if (group != self.defaultDelegation['group'] and proxyOptsSetPlace['group']):
                        msgadd1.append("VO group '%s'" % (group))
                    if (role  != self.defaultDelegation['role']  and proxyOptsSetPlace['role'] ):
                        msgadd1.append("VO role '%s'" % (role))
                    msgadd2 = []
                    if (group != self.defaultDelegation['group'] and proxyOptsSetPlace['group']):
                        msgadd2.append("VO group '%s'" % (self.defaultDelegation['group']))
                    if (role  != self.defaultDelegation['role']  and proxyOptsSetPlace['role'] ):
                        msgadd2.append("VO role '%s'" % (self.defaultDelegation['role']))
                    msg  = "Proxy file %s exists with %s, but you have specified to use %s." \
                           % (proxyfilename, " and ".join(msgadd1), " and ".join(msgadd2))
                    msg += "\nDo you want to overwrite the proxy (Y/N)?"
                    self.logger.info(msg)
                    answer = raw_input()
                    if answer in ['y', 'Y', 'n', 'N']:
                        break
                ## If the user wants to overwrite the proxy, then we do it. Otherwise
                ## we exit asking the user to modify the config or command line option(s).
                if answer.upper() == 'Y':
                    self.proxyChanged = True
                if answer.upper() == 'N':
                    msgadd = []
                    if (group != self.defaultDelegation['group'] and proxyOptsSetPlace['group'] in ['config']) or \
                       (role  != self.defaultDelegation['role']  and proxyOptsSetPlace['role']  in ['config']):
                        msg1add = []
                        if (group != self.defaultDelegation['group'] and proxyOptsSetPlace['group'] in ['config']):
                            msg1add.append("User.voGroup")
                        if (role  != self.defaultDelegation['role']  and proxyOptsSetPlace['role']  in ['config']):
                            msg1add.append("User.voRole")
                        plural = "s" if len(msg1add) > 1 else ""
                        msg1 = "please modify the %s parameter%s in the CRAB configuration file to match the existing proxy" \
                               % (" and ".join(msg1add), plural)
                        msgadd.append(msg1)
                    if (group != self.defaultDelegation['group'] and proxyOptsSetPlace['group'] in ['cmdopts', 'cache']) or \
                       (role  != self.defaultDelegation['role']  and proxyOptsSetPlace['role']  in ['cmdopts', 'cache']):
                        msg2add = []
                        if (group != self.defaultDelegation['group'] and proxyOptsSetPlace['group'] in ['cmdopts', 'cache']):
                            msg2add.append("--voGroup")
                        if (role  != self.defaultDelegation['role']  and proxyOptsSetPlace['role']  in ['cmdopts', 'cache']):
                            msg2add.append("--voRole")
                        plural = "s" if len(msg2add) > 1 else ''
                        msg2 = "please specify the %s command line option%s with the same value%s as in your proxy" \
                               % (" and ".join(msg2add), plural, plural)
                        msgadd.append(msg2)
                    msg = "Then, %s." % (" and ".join(msgadd))
                    raise ProxyCreationException(msg)

        ## Create a new proxy if the current one is expired or if we were instructed
        ## to change the proxy for a new one.
        if proxytimeleft < timeleftthreshold or self.proxyChanged:
            msg  = "Creating new proxy for %s hours" % (self.defaultDelegation['proxyValidity'])
            msg += " with VO group '%s' and VO role '%s'." % (self.defaultDelegation['group'], self.defaultDelegation['role'])
            self.logger.debug(msg)
            ## Create the proxy.
            proxy.create()
            ## Check that the created proxy has the expected VO group and role (what
            ## we have set in the defaultDelegation dictionary).
            proxytimeleft = proxy.getTimeLeft()
            group, role = proxy.getUserGroupAndRoleFromProxy(proxy.getProxyFilename())
            if proxytimeleft > 0 and group == self.defaultDelegation['group'] and role == self.defaultDelegation['role']:
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
        myproxy = Proxy (self.defaultDelegation)
        myproxy.userDN = myproxy.getSubjectFromCert(self.certLocation)

        myproxytimeleft = 0
        self.logger.debug("Getting myproxy life time left for %s" % self.defaultDelegation["myProxySvr"])
        # return an integer that indicates the number of seconds to the expiration of the proxy in myproxy
        myproxytimeleft = myproxy.getMyProxyTimeLeft(serverRenewer=True, nokey=nokey)
        self.logger.debug("Myproxy is valid: %i" % myproxytimeleft)

        trustRetrListChanged = (myproxy.trustedRetrievers != self.defaultDelegation['serverDN']) #list on the REST and on myproxy are different
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

