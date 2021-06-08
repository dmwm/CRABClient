"""
Contains the logic and wraps calls to WMCore.Credential.Proxy
"""
import logging
import os
from hashlib import sha1

from CRABClient.ProxyInteractions import VomsProxy, MyProxy
from CRABClient.ClientExceptions import ProxyCreationException, EnvironmentException
from CRABClient.ClientUtilities import colors, StopExecution


class CredentialInteractions(object):
    '''
    CredentialInteraction

    Takes care of wrapping Proxy interaction and defining common behaviour
    for all the client commands.
    '''
    def __init__(self, retrievers=None, myproxy='myproxy.cern.ch', role='', group='', logger=None):
        '''
        Constructor
        '''

        self.myproxyDesiredValidity = 30 ## days
        self.logger = logger
        self.defaultDelegation = {
                                  #do not print messages coming from Proxy library, but put them into the logfile
                                  'logger':          logging.getLogger('CRAB3'),
                                  'vo':              'cms',
                                  'myProxyServer':   'myproxy.cern.ch',
                                  'proxyValidity'  : '172:00', ## hh:mm
                                  'myproxyValidity': '%i:00' % (self.myproxyDesiredValidity*24), ## hh:mm
                                  'retrievers':      retrievers,
                                  'group' :          group,
                                  'role':            role if role != '' else 'NULL',
                                  }
        self.proxyChanged = False
        self.certLocation = '~/.globus/usercert.pem' if 'X509_USER_CERT' not in os.environ else os.environ['X509_USER_CERT']
        self.proxyFile = '/tmp/x509up_u%d' % os.getuid() if 'X509_USER_PROXY' not in os.environ else os.environ['X509_USER_PROXY']


    def setVOGroupVORole(self, group, role):
        self.defaultDelegation['group'] = group
        self.defaultDelegation['role'] = role if role != '' else 'NULL'

    def setMyProxyValidity(self, validity):
        """
        set desired validity for credential in myproxy in minutes
        args: validity: integer
        """
        self.defaultDelegation['myproxyValidity'] = '%i:00' % (validity/60)  # from minutes to hh:mm

    def setRetrievers(self, retrievers):
        self.defaultDelegation['retrievers'] = retrievers


    def setMyProxyServer(self, server):
        self.defaultDelegation['myProxyServer'] = server

    def vomsProxy(self):
        try:
            vp = VomsProxy(logger=self.defaultDelegation['logger'])
            vp.setVOGroupVORole(group=self.defaultDelegation['group'], role=self.defaultDelegation['role'])
        except ProxyCreationException as ex:
            self.logger.debug(ex)
            raise EnvironmentException('Problem with Grid environment: %s ' % str(ex))
        return vp

    def myProxy(self):
        mp = MyProxy(logger=self.defaultDelegation['logger'], username=self.defaultDelegation['username'])
        return mp

    def getFilename(self):
        return self.proxyFile

    def createNewVomsProxy(self, timeLeftThreshold=0, doProxyGroupRoleCheck=False, proxyCreatedByCRAB=False, proxyOptsSetPlace=None):
        """
        Handles the proxy creation:
           - checks if a valid proxy still exists
           - performs the creation if it is expired
           - returns a dictionary with keys: filename timelect actimeleft group role userdn
        """
        proxyInfo = {}
        ## TODO add the change to have user-cert/key defined in the config.
        #proxy = self.vomsProxy()
        try:
            proxy = VomsProxy(logger=self.defaultDelegation['logger'])
            proxy.setVOGroupVORole(group=self.defaultDelegation['group'], role=self.defaultDelegation['role'])
        except ProxyCreationException as ex:
            self.logger.debug(ex)
            raise EnvironmentException('Problem with Grid environment: %s ' % str(ex))
        self.logger.debug("Checking credentials")
        proxyFileName = proxy.getFilename()
        proxyInfo['filename'] = proxyFileName
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
            group, role = proxy.getGroupAndRole()
            proxyAttrs = {'group': group, 'role': role}
            ## Make sure proxyOptsSetPlace is a dictionary and has the expected keys.
            if not isinstance(proxyOptsSetPlace, dict):
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
                                    msgadd2.append("you have not explicitely specified %s to use " % (setmsg) + \
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
                            msg = "Then please modify the CRAB configuration file"
                            msg += " in order to match the existing proxy."
                        elif proxyOptsSetPlace['for_set_use'] == "cmdopts":
                            msg = "Then please specify the crab command options --voGroup and/or --voRole"
                            msg += " in order to match the existing proxy."
                            msg += "\nNote: If the options --voGroup/--voRole are not specified,"
                            msg += " the defaults ''/'NULL' are assumed."
                        else: ## Should never get into this branch of the if, but just in case.
                            msg = "Then please modify the VO group and/or VO role in the place"
                            msg += " you have specified them in order to match the existing proxy."
                        self.logger.info(msg)
                        raise StopExecution

        ## Create a new proxy if the current one is expired or if we were instructed
        ## to change the proxy for a new one.
        proxyInfo['timeleft'] = proxyTimeLeft
        if proxyTimeLeft < timeLeftThreshold or self.proxyChanged:
            msg = "Creating new proxy for %s hours" % (self.defaultDelegation['proxyValidity'])
            msg += " with VO group '%s' and VO role '%s'." % (self.defaultDelegation['group'], self.defaultDelegation['role'])
            self.logger.debug(msg)
            ## Create the proxy.
            proxy.create()
            ## Check that the created proxy has the expected VO group and role (what
            ## we have set in the defaultDelegation dictionary).
            proxyTimeLeft = proxy.getTimeLeft()
            proxyInfo['timeleft'] = proxyTimeLeft
            group, role = proxy.getGroupAndRole()
            proxyInfo['group'] = group
            proxyInfo['role'] = role
            if proxyTimeLeft > 0 and group == self.defaultDelegation['group'] and role == self.defaultDelegation['role']:
                self.logger.debug("Proxy created.")
            else:
                raise ProxyCreationException("Problems creating proxy.")

        return proxyInfo


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

        :returns a tupla with info in the credential in myprosxy: (credentialName, myproxytimeleft)
            credentialName : username to use in myproxy -l username
            myproxytimeleft: validity of the credential in seconds
        """
        # create a WMCore/Proxy object to get DN
        proxy = VomsProxy(logger=self.defaultDelegation['logger'])
        userDNFromProxy = proxy.getSubject()
        # now use that to compute the credential name to pass in input to a new Proxy object
        credentialName = sha1(userDNFromProxy).hexdigest()
        myproxy = MyProxy(logger=self.defaultDelegation['logger'])
        myproxyDesiredValidity = self.defaultDelegation['myproxyValidity']

        myproxytimeleft = 0
        self.logger.debug("Getting myproxy life time left for %s" % credentialName)
        # return an integer that indicates the number of seconds to the expiration of the proxy in myproxy
        # Also catch the exception in case WMCore encounters a problem with the proxy itself (one such case was #4532)
        try:
            myproxytimeleft, trustedRetrievers = myproxy.getInfo(username=credentialName)
        except Exception as ex:
            logging.exception("Problems calculating proxy lifetime, logging stack trace and raising ProxyCreationException")
            # WMException may contain the _message attribute. Otherwise, take the exception as a string.
            msg = ex._message if hasattr(ex, "_message") else str(ex)  # pylint: disable=protected-access, no-member
            raise ProxyCreationException("Problems calculating the time left until the expiration of the proxy.\n" +
                                         " Please reset your environment or contact hn-cms-computing-tools@cern.ch if the problem persists.\n%s" % msg)
        self.logger.debug("Myproxy is valid: %i" % myproxytimeleft)

        trustRetrListChanged = trustedRetrievers != self.defaultDelegation['retrievers'] #list on the REST and on myproxy are different
        if myproxytimeleft < timeleftthreshold or trustRetrListChanged:
            # checking the enddate of the user certificate
            usercertDaysLeft = myproxy.getUserCertEndDate()
            if usercertDaysLeft == 0:
                msg = "%sYOUR USER CERTIFICATE IS EXPIRED (OR WILL EXPIRE TODAY)." % colors.RED
                msg += " YOU CANNOT USE THE CRAB3 CLIENT."
                msg += " PLEASE REQUEST A NEW CERTIFICATE HERE https://gridca.cern.ch/gridca/"
                msg += " AND SEE https://ca.cern.ch/ca/Help/?kbid=024010%s" % colors.NORMAL
                raise ProxyCreationException(msg)

            #if the certificate is going to expire print a warning. This is going to bre printed at every command if
            #the myproxytimeleft is inferior to the timeleftthreshold
            if usercertDaysLeft < self.myproxyDesiredValidity:
                msg = "%sYour user certificate is going to expire in %s days." % (colors.RED, usercertDaysLeft)
                msg += " See: https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookStartingGrid#ObtainingCert %s" % colors.NORMAL
                self.logger.info(msg)
                #check if usercertDaysLeft ~= myproxytimeleft which means we already delegated the proxy for as long as we could
                if abs(usercertDaysLeft*60*60*24 - myproxytimeleft) < 60*60*24 and not trustRetrListChanged: #less than one day between usercertDaysLeft and myproxytimeleft
                    return (credentialName, myproxytimeleft)
                #adjust the myproxy delegation time accordingly to the user cert validity
                self.logger.info("%sDelegating your proxy for %s days instead of %s %s",
                                 colors.RED, usercertDaysLeft, self.myproxyDesiredValidity, colors.NORMAL)
                myproxyDesiredValidity = "%i:00" % (usercertDaysLeft*24)

            # creating the proxy
            self.logger.debug("Delegating a myproxy for %s hours", myproxyDesiredValidity)
            try:
                myproxy.create(username=credentialName, retrievers=self.defaultDelegation['retrievers'],
                               validity=myproxyDesiredValidity)
                myproxytimeleft, _ = myproxy.getInfo(username=credentialName)
                if myproxytimeleft <= 0:
                    raise ProxyCreationException("It seems your proxy has not been delegated to myproxy. Please check the logfile for the exact error "+\
                                                            "(Maybe you simply typed a wrong password)")
                else:
                    self.logger.debug("My-proxy delegated.")
            except Exception as ex:
                msg = ex._message if hasattr(ex, '_message') else str(ex)  # pylint: disable=protected-access, no-member
                raise ProxyCreationException("Problems delegating My-proxy. %s" % msg)
        return (credentialName, myproxytimeleft)
