"""
Contains the logic and wraps calls to WMCore.Credential.Proxy
"""
import logging
import os

from WMCore.Credential.Proxy import Proxy, CredentialException
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from CRABClient.ClientExceptions import ProxyCreationException, EnvironmentException
from CRABClient.ClientUtilities import colors, StopExecution


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
        Return user name from DN
        """
        proxy = self.proxy()
        return proxy.getUserName()


    def getFilename(self):
        proxy = self.proxy()
        proxyFileName = proxy.getProxyFilename()
        if not os.path.isfile(proxyFileName):
            self.logger.debug("Proxy file %s not found" % (proxyFileName))
            return ''
        return proxyFileName


    def getTimeLeft(self):
        proxy = self.proxy()
        return proxy.getTimeLeft() ## Returns an integer that indicates the number of seconds to the expiration of the proxy


    def createNewVomsProxySimple(self, timeLeftThreshold = 0):
        proxy = self.proxy()
        self.logger.debug("Checking credentials")
        proxyFileName = proxy.getProxyFilename()
        if not os.path.isfile(proxyFileName):
            self.logger.debug("Proxy file %s not found" % (proxyFileName))
            proxyTimeLeft = 0
        else:
            self.logger.debug("Found proxy file %s" % (proxyFileName))
            self.logger.debug("Getting proxy life time left")
            proxyTimeLeft = proxy.getTimeLeft()
            hours, minutes, seconds = int(proxyTimeLeft/3600), int((proxyTimeLeft%3600)/60), int((proxyTimeLeft%3600)%60)
            self.logger.debug("Proxy valid for %02d:%02d:%02d hours" % (hours, minutes, seconds))
        ## Create a new proxy if the current one is expired or if we were instructed
        ## to change the proxy for a new one.
        proxyCreated = False
        if proxyTimeLeft < timeLeftThreshold:
            msg  = "Creating new proxy for %s hours" % (self.defaultDelegation['proxyValidity'])
            msg += " with VO group '%s' and VO role '%s'." % (self.defaultDelegation['group'], self.defaultDelegation['role'])
            self.logger.debug(msg)
            ## Create the proxy.
            proxy.create()
            ## Check that the created proxy has the expected VO group and role (what we have
            ## set in the defaultDelegation dictionary).
            proxyTimeLeft = proxy.getTimeLeft()
            group, role = proxy.getUserGroupAndRoleFromProxy(proxy.getProxyFilename())
            if proxyTimeLeft > 0 and group == self.defaultDelegation['group'] and role == self.defaultDelegation['role']:
                self.logger.debug("Proxy created.")
                proxyCreated = True
            else:
                raise ProxyCreationException("Problems creating proxy.")
           
        return proxyCreated


    def createNewVomsProxy(self, timeLeftThreshold = 0, doProxyGroupRoleCheck = False, proxyCreatedByCRAB = False, proxyOptsSetPlace = None):
        """
        Handles the proxy creation:
           - checks if a valid proxy still exists
           - performs the creation if it is expired
        """
        ## TODO add the change to have user-cert/key defined in the config.
        proxy = self.proxy()
        ## Not sure if proxy.userDN is used. We don't have this in createNewVomsProxySimple...
        proxy.userDN = proxy.getSubjectFromCert(self.certLocation)
        self.logger.debug("Checking credentials")
        proxyFileName = proxy.getProxyFilename()
        if not os.path.isfile(proxyFileName):
            self.logger.debug("Proxy file %s not found" % (proxyFileName))
            proxyTimeLeft = 0
        else:
            self.logger.debug("Found proxy file %s" % (proxyFileName))
            self.logger.debug("Getting proxy life time left")
            proxyTimeLeft = proxy.getTimeLeft()
            hours, minutes, seconds = int(proxyTimeLeft/3600), int((proxyTimeLeft%3600)/60), int((proxyTimeLeft%3600)%60)
            self.logger.debug("Proxy valid for %02d:%02d:%02d hours" % (hours, minutes, seconds))

        if doProxyGroupRoleCheck:
            ## Get the VO group and role in the proxy.
            group, role = proxy.getUserGroupAndRoleFromProxy(proxyFileName)
            proxyAttrs = {'group': group, 'role': role}
            ## Make sure proxyOptsSetPlace is a dictionary and has the expected keys.
            if type(proxyOptsSetPlace) != dict:
                proxyOptsSetPlace = {}
            proxyOptsSetPlace.setdefault('set_in', {})
            proxyOptsSetPlace['set_in'].setdefault('group', "")
            proxyOptsSetPlace['set_in'].setdefault('role', "")
            proxyOptsSetPlace.setdefault('for_set_use', "")
            ## If the proxy (not created by CRAB) is not expired, ...
            if (proxyTimeLeft > timeLeftThreshold) and not proxyCreatedByCRAB and \
               (self.defaultDelegation['group'] != None and self.defaultDelegation['role'] != None):
                ## ... we check if the VO group and VO role in the proxy are the same as the
                ## ones in defaultDelegation (which were either specified by the user in the
                ## CRAB configuration file or in the command options, or were taken from the
                ## request cache, or otherwise were given default values).
                if (group != self.defaultDelegation['group'] or role != self.defaultDelegation['role']):
                    ## If they are not the same, force the user to either create a new proxy with
                    ## the same VO group and VO role as in defaultDelegation, or to change the VO
                    ## group/role in the configuration file (or in the command options, whatever
                    ## corresponds).
                    msgadd1, msgadd2, previous = [], [], ""
                    for attr in ['group', 'role']:
                        msgadd1.append("VO %s '%s'" % (attr, proxyAttrs[attr]))
                        if proxyAttrs[attr] != self.defaultDelegation[attr]:
                            if proxyOptsSetPlace['set_in'][attr] == previous:
                                if proxyOptsSetPlace['set_in'][attr] in ["config", "cmdopts", "cache"]:
                                    msgadd2.append("VO %s '%s'" % (attr, self.defaultDelegation[attr]))
                                else:
                                    msgadd2.append("VO %s '%s'" % (attr, proxyAttrs[attr]))
                            else:
                                previous = proxyOptsSetPlace['set_in'][attr]
                                if proxyOptsSetPlace['set_in'][attr] == "config":
                                    msgadd2.append("in the CRAB configuration file you have specified to use " + \
                                                   "VO %s '%s'" % (attr, self.defaultDelegation[attr]))
                                elif proxyOptsSetPlace['set_in'][attr] == "cmdopts":
                                    msgadd2.append("in the crab command options you have specified to use " + \
                                                   "VO %s '%s'" % (attr, self.defaultDelegation[attr]))
                                elif proxyOptsSetPlace['set_in'][attr] == "cache":
                                    msgadd2.append("in the .requestcache file, inside the given CRAB project directory, it says that this task used " + \
                                                   "VO %s '%s'" % (attr, self.defaultDelegation[attr]))
                                else:
                                    setmsg = ""
                                    if proxyOptsSetPlace['for_set_use'] == "config":
                                        setmsg = "(in the CRAB configuration file) "
                                    elif proxyOptsSetPlace['for_set_use'] == "cmdopts":
                                        setmsg = "(in the crab command options) "
                                    msgadd2.append("you have not explicitely specified %sto use " % (setmsg) + \
                                                   "VO %s '%s'" % (attr, proxyAttrs[attr]))
                    msg = "Proxy file %s exists with %s, but %s." % (proxyFileName, " and ".join(msgadd1), " and ".join(msgadd2))
                    self.logger.info(msg)
                    while True:
                        msg = "Do you want to overwrite the proxy (Y/N)?"
                        self.logger.info(msg)
                        answer = raw_input()
                        if answer in ['y', 'Y', 'n', 'N']:
                            msg = "Answer was: %s" % (answer)
                            self.logger.debug(msg)
                            break
                    if answer.upper() == 'Y':
                        self.proxyChanged = True
                    if answer.upper() == 'N':
                        if proxyOptsSetPlace['for_set_use'] == "config":
                            msg  = "Then please modify the CRAB configuration file"
                            msg += " in order to match the existing proxy."
                        elif proxyOptsSetPlace['for_set_use'] == "cmdopts":
                            msg  = "Then please specify the crab command options --voGroup and/or --voRole"
                            msg += " in order to match the existing proxy."
                            msg += "\nNote: If the options --voGroup/--voRole are not specified,"
                            msg += " the defaults ''/'NULL' are assumed."
                        else: ## Should never get into this branch of the if, but just in case.
                            msg  = "Then please modify the VO group and/or VO role in the place"
                            msg += " you have specified them in order to match the existing proxy."
                        self.logger.info(msg)
                        raise StopExecution

        ## Create a new proxy if the current one is expired or if we were instructed
        ## to change the proxy for a new one.
        if proxyTimeLeft < timeLeftThreshold or self.proxyChanged:
            msg  = "Creating new proxy for %s hours" % (self.defaultDelegation['proxyValidity'])
            msg += " with VO group '%s' and VO role '%s'." % (self.defaultDelegation['group'], self.defaultDelegation['role'])
            self.logger.debug(msg)
            ## Create the proxy.
            proxy.create()
            ## Check that the created proxy has the expected VO group and role (what
            ## we have set in the defaultDelegation dictionary).
            proxyTimeLeft = proxy.getTimeLeft()
            group, role = proxy.getUserGroupAndRoleFromProxy(proxy.getProxyFilename())
            if proxyTimeLeft > 0 and group == self.defaultDelegation['group'] and role == self.defaultDelegation['role']:
                self.logger.debug("Proxy created.")
            else:
                raise ProxyCreationException("Problems creating proxy.")

        return proxy.getProxyFilename()


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
                self.logger.info("%sYour user certificate is going to expire in %s days.  https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookStartingGrid#ObtainingCert %s"\
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

