import sys
import os
import string
import time
import signal
import commands
import urllib
import select
import fcntl
import tempfile
from subprocess import Popen, PIPE, STDOUT

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import MissingOptionException, ClientException

class checkHNname(SubCommand):
    """
    Let the user to test the extraction of his/her HN username from SiteDB.
    """
    name = 'checkHNname'

    def __call__(self):

        if self.options.standalone:
            return self.standaloneCheck()
        else:
            return self.crabCheck()


    def terminate(self, exitcode):
        pass


    def standaloneCheck(self):

        self.logger.info('Starting check in standalone mode...')
        # direct stderr from voms-proxy-* to dev/null to avoid stupid Java messages :-(
        status, dn = commands.getstatusoutput('eval `scram unsetenv -sh`; voms-proxy-info -identity 2>/dev/null')
        if status == 0:
           self.logger.info('Your DN is: %s' % dn)
        status, proxy_file = commands.getstatusoutput('eval `scram unsetenv -sh`; voms-proxy-info -path 2>/dev/null')
        if status:
            self.logger.info('ERROR getting proxy path')
        os.environ['X509_USER_PROXY'] = proxy_file
        if not 'X509_CERT_DIR' in os.environ:
            os.environ['X509_CERT_DIR'] = '/etc/grid-security/certificates'
        cmd  = "curl -s --capath $X509_CERT_DIR --cert $X509_USER_PROXY --key $X509_USER_PROXY 'https://cmsweb.cern.ch/sitedb/data/prod/whoami'"
        cmd += " | tr ':,' '\n'"
        cmd += " | grep -A1 login"
        cmd += " | tail -1"
        status, hn_username = commands.getstatusoutput(cmd)
        hn_username = string.strip(hn_username)
        hn_username = hn_username.replace('"','')
        self.logger.info('Your HN username is: %s' % hn_username)
        self.logger.info('Finished')

        return {'DN' : dn , 'HNusername' : hn_username}

    def crabCheck(self):

        ## Do we need to add something here?

        self.logger.info('Starting check in CRAB-like mode...')
        dn = self.getDN()
        self.logger.info('Your DN is: %s' % dn)
        hn_username = None
        try:
            hn_username = self.getHNUsernameFromSiteDB()
            self.logger.info('Your HN username is: %s\n' % hn_username)
        except:
            self.logger.info('WARNING native crab_utils failed!')
            dn = urllib.urlencode({'dn': self.getDN()})
            self.logger.info('Trying now using urlencoded DN: \n\t %s' % dn)
            status, hn_username = self.getHNUsernameFromSiteDB_urlenc(dn)
            if status == 1:
                self.logger.info('WARNING: failed also using urlencoded DN')
            else:
                self.logger.info('Your HN username is: %s' % hn_username)
                self.logger.info('problems with crab_utils')
        self.logger.info('Finished')

        return {'DN' : dn , 'HNusername' : hn_username}

    def getDN(self):
        """
        Extract DN from user proxy's identity.
        """
        try:
            user_dn = self.runCommand("eval `scram unsetenv -sh`; voms-proxy-info -identity")
            user_dn = string.strip(user_dn)
            #remove /CN=proxy that could cause problems with siteDB check at server-side
            user_dn = user_dn.replace('/CN=proxy', '')
            #search for a / to avoid picking up warning messages
            user_dn = user_dn[user_dn.find('/'):]
        except:
            msg = 'Error. Problem with voms-proxy-info -identity command'
            raise ClientException(msg)
        return user_dn.split('\n')[0]


    def getHNUsernameFromSiteDB(self):
        """
        Extract HN username from SiteDB.
        """
        hn_username = None
        user_dn = self.getDN()
        sitedb = SiteDBJSON()
        msg  = "Error extracting user name from SiteDB:\n"
        msg += " If problem persists check that you are registered in SiteDB, see https://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB\n"
        msg += " and follow the diagnostics steps indicated there at"
        msg += " https://twiki.cern.ch/twiki/bin/viewauth/CMS/SiteDBForCRAB#Check_username_extraction_from_s"
        try:
            hn_username = sitedb.dnUserName(dn = user_dn)
            # cast to a string, for odd reasons new
            # WMCore/Services/SiteDB/SiteDB.py returns unicode
            # even if cached file seems to have plain strings
            # unicode in user name has bad effects on some old
            # code e.g. in crab uploadlog
            hn_username = str(hn_username)
        except Exception, text:
            raise ClientException(msg)
        if not hn_username:
            raise ClientException(msg)
        return hn_username


    def getHNUsernameFromSiteDB_urlenc(self, dn):

        hn_username = None
        user_dn = dn
        sitedb = SiteDBJSON()
        status = 0
        try:
            hn_username = sitedb.dnUserName(dn = user_dn)
        except:
            status = 1
        return status, hn_username


    def setPgid(self):
        """
        preexec_fn for Popen to set subprocess pgid
        """
        os.setpgid(os.getpid(), 0)


    def runCommand(self, command, printout = 0, timeout = 30., errorCode = False):
        """
        Execute the command provided in a popen object with a timeout.
        """
        start = time.time()
        p = Popen(command, shell = True, stdin = PIPE, stdout = PIPE, stderr = STDOUT, close_fds = True, preexec_fn = self.setPgid)

        # playing with fd
        fd = p.stdout.fileno()
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        # return values
        timedOut = False
        outc = []

        while True:
            (r, w, e) = select.select([fd], [], [], timeout)
            if fd not in r:
                timedOut = True
                break
            read = p.stdout.read()
            if read != '':
                outc.append(read)
            else:
                break

        if timedOut:
            self.logger.info('Command %s timed out after %d sec' % (command, int(timeout)))
            stop = time.time()
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                os.kill(p.pid, signal.SIGKILL)
                p.wait()
                p.stdout.close()
            except OSError, err:
                self.logger.info('Warning: an error occurred killing subprocess [%s]' % str(err))
            raise ClientException("Timeout")

        try:
            p.wait()
            p.stdout.close()
        except OSError, err:
            self.logger.info('Warning: an error occurred closing subprocess [%s] %s %s' % (str(err), ''.join(outc), p.returncode))

        returncode = p.returncode
        if returncode:
            msg = 'Command: %s \n failed with exit code %s \n' % (command, returncode)
            msg += str(''.join(outc))
            if not errorCode:
                self.logger.info(msg)
                return None
        if errorCode:
            if returncode is None:
                returncode = -66666
            return returncode, ''.join(outc)

        return ''.join(outc)


    def setOptions(self):
        """
        __setOptions__
        This allows to set specific command options.
        """
        self.parser.add_option('--standalone',
                               dest = 'standalone',
                               action = "store_true",
                               default = False,
                               help = 'Check HN username extraction from SiteDB outside crab.')


    def validateOptions(self):
        SubCommand.validateOptions(self)

        if not self.options.standalone:
            self.logger.info("Currently only the standalone check is implemented.")
            self.logger.info("If you want to do a standalone check, do 'crab %s --standalone'" % self.name)
            raise MissingOptionException
