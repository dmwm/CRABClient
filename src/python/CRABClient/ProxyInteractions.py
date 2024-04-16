"""
Provides utilities do deal with voms-proxy* and myproy-*
Meant to replace use of WMCore/Credential/Proxy
But differently from that, use two different objects to interact with
voms-proxy-* or myproxy-* since, this will make things more clear and
coder more readable
"""
import os
from datetime import datetime

from CRABClient.ClientUtilities import execute_command
from CRABClient.ClientExceptions import ProxyCreationException

class VomsProxy(object):
    def __init__(self, logger=None):
        """
        Constructor, sets sensible defaults for everything
        """
        vomsDesiredValidDays = 8
        self.proxyChanged = False
        self.certLocation = '~/.globus/usercert.pem' if 'X509_USER_CERT' not in os.environ else os.environ['X509_USER_CERT']
        self.keyLocation = '~/.globus/userkey.pem' if 'X509_USER_KEY' not in os.environ else os.environ['X509_USER_KEY']
        self.proxyFile = '/tmp/x509up_u%d' % os.getuid() if 'X509_USER_PROXY' not in os.environ else os.environ['X509_USER_PROXY']
        self.logger = logger
        self.DN = ''
        self.desiredValidity = '%i:00' % (vomsDesiredValidDays*24)  # from days to hh:mm
        self.timeleft = 0
        self.group = ''
        self.role = 'NULL'

    def create(self, timeLeftThreshold=720):
        # is there a proxy already ?
        # does it have correct group and role ?
        # is it valid long enough ?
        # all OK, do nothing
        # need a new proxy
        cmd = 'voms-proxy-init --rfc'
        cmd += ' --cert %s' % self.certLocation
        cmd += ' --key %s' % self.keyLocation
        cmd += ' --out %s' % self.proxyFile
        cmd += ' --valid %s' % self.desiredValidity
        vomsString = 'cms:/cms'
        if self.group:
            vomsString += '/%s' % self.group
        if self.role and self.role != 'NULL':
            vomsString += '/Role=%s' % self.role
        cmd += ' --voms %s' % vomsString
        stdout, stderr, rc = execute_command(cmd, logger=self.logger, redirect=False)
        if rc != 0:
            self.logger.error(stdout+'\n'+stderr)
            msg = "\n".join(['Error executing %s:' % cmd, stdout, stderr])
            raise ProxyCreationException(msg)

    def setVOGroupVORole(self, group, role):
        self.group = group
        self.role = role if role != '' else 'NULL'

    def getFilename(self):
        return self.proxyFile

    def validateVO(self):
        # make sure that proxy has a VOMS extension for CMS VirtualOrganization
        cmd = 'voms-proxy-info --vo --file %s' % self.proxyFile
        stdout, stderr, rc = execute_command(cmd, logger=self.logger)
        if rc != 0 or 'cms' not in stdout:
            msg = "\n".join(['Error executing %s:' % cmd, stdout, stderr])
            self.logger.error(msg)
            msg = 'proxy %s is not a valid proxy file or has no valid VOMS extension.\n' % self.proxyFile
            stdout, stderr, rc = execute_command('voms-proxy-info -all', logger=self.logger)
            msg += 'output of voms-proxy-info -all is\n%s' % (stdout+'\n'+stderr)
            msg += '\n**** Make sure you do voms-proxy-init -voms cms ****\n'
            raise ProxyCreationException(msg)

    def getTimeLeft(self):
        cmd = 'voms-proxy-info --actimeleft --timeleft --file %s' % self.proxyFile
        stdout, stderr, rc = execute_command(cmd, logger=self.logger)
        if rc != 0:
            self.logger.error(stdout+'\n'+stderr)
            msg = "\n".join(['Error executing %s:' % cmd, stdout, stderr])
            raise ProxyCreationException(msg)

        # pick the shorter between actimeleft and timeleft
        times = stdout.split('\n')
        timeLeft = min(int(times[0]), int(times[1]))
        return timeLeft

    def getGroupAndRole(self):
        cmd = 'voms-proxy-info --fqan --file %s' % self.proxyFile
        stdout, stderr, rc = execute_command(cmd, logger=self.logger)
        if rc != 0:
            self.logger.error(stdout+'\n'+stderr)
            msg = "\n".join(['Error executing %s:' % cmd, stdout, stderr])
            raise ProxyCreationException(msg)
        fqans = str(stdout)
        primaryFqan = fqans.split('\n')[0]
        attributes = primaryFqan.split('/')
        if len(attributes) > 4:
            group = attributes[2]
            role = attributes[3].split('=')[1]
        else:
            group = ''
            role = attributes[2].split('=')[1]
        return group, role

    def getSubject(self):
        cmd = 'voms-proxy-info --identity --file %s' % self.proxyFile
        stdout, stderr, rc = execute_command(cmd, logger=self.logger)
        if rc != 0:
            self.logger.error(stdout+'\n'+stderr)
            msg = "\n".join(['Error executing %s:' % cmd, stdout, stderr])
            raise ProxyCreationException(msg)
        return stdout.rstrip()


class MyProxy(object):
    """
    an object to interact with myproxy-* commands
    """

    def __init__(self, username=None, logger=None):
        """
        Constructor, sets sensible defaults for everything
        """
        self.certLocation = '~/.globus/usercert.pem' if 'X509_USER_CERT' not in os.environ else os.environ['X509_USER_CERT']
        self.keyLocation = '~/.globus/userkey.pem' if 'X509_USER_KEY' not in os.environ else os.environ['X509_USER_KEY']
        self.logger = logger
        self.timeleft = 0
        self.username = username

    def create(self, username=None, retrievers=None, validity=720):
        """
        creates a new credential in myproxy.cern.ch
        args: username: string: the username of the credential, usually the has of the user DN
        args: retrievers: string: regexp indicating list of DN's authorized to retrieve this credential
        args: validity: integer: how long this credential will be valid for in hours, default is 30 days
        example of the command we want :
                 command : export GT_PROXY_MODE=rfc
                 myproxy-init -d -n -s myproxy.cern.ch
                 -x -R '/DC=ch/DC=cern/OU=computers/CN=vocms0105.cern.ch|/DC=ch/DC=cern/OU=computers/CN=crab-(preprod|prod|dev)-tw(02|01).cern.ch|/DC=ch/DC=cern/OU=computers/CN=(ddi|ddidk|mytw).cern.ch|/DC=ch/DC=cern/OU=computers/CN=stefanov(m|m2).cern.ch'
                 -x -Z '/DC=ch/DC=cern/OU=computers/CN=vocms0105.cern.ch|/DC=ch/DC=cern/OU=computers/CN=crab-(preprod|prod|dev)-tw(02|01).cern.ch|/DC=ch/DC=cern/OU=computers/CN=(ddi|ddidk|mytw).cern.ch|/DC=ch/DC=cern/OU=computers/CN=stefanov(m|m2).cern.ch'
                 -l 'be1f4dc5be8664cbd145bf008f5399adf42b086f'
                 -t 168:00 -c 3600:00
        """
        cmd = 'export GT_PROXY_MODE=rfc ; myproxy-init -d -n -s myproxy.cern.ch'
        cmd += ' -C %s' % self.certLocation
        cmd += ' -y %s' % self.keyLocation
        cmd += ' -x -R \'%s\'' % retrievers
        cmd += ' -x -Z \'%s\'' % retrievers
        cmd += ' -l %s' % username
        cmd += ' -t 168 -c %s' % validity  # validity of the retrieved proxy: 7 days = 168 hours
        stdout, stderr, rc = execute_command(cmd, logger=self.logger)
        if rc != 0:
            self.logger.error(stdout+'\n'+stderr)
            msg = "\n".join(['Error executing %s:' % cmd, stdout, stderr])
            raise ProxyCreationException(msg)

    def getInfo(self, username=None):
        """
        returns information about a credential stored in myproxy.cern.ch
        args: username: string: the username of the credential, usually the has of the user DN
        """
        cmd = 'myproxy-info -s myproxy.cern.ch -l %s' % username
        stdout, stderr, rc = execute_command(cmd, logger=self.logger)
        if rc != 0:
            self.logger.error(stdout+'\n'+stderr)
        if rc > 0 or not stdout:  # if there's no credential myproxy-info returns rc=1
            return 0, ''
        olines = stdout.rstrip().split('\n')
        trustedRetrievalPolicy = olines[-2]
        # allow for ':' in the trustedRetrievers DN's (as for robot cert !)
        # by taking everything after the first ':' in myproxy-info output
        # split(':', maxsplit=1) would be more clear, but it is not allowed in python2
        trustedRetrievers = trustedRetrievalPolicy.split(':', 1)[1].strip()
        times = olines[-1].split(':')
        hours = int(times[1])
        mins = int(times[2])
        timeLeft = hours*3600 + mins*60  # let's ignore seconds
        return timeLeft, trustedRetrievers

    def getUserCertEndDate(self):
        """
        Return the number of seconds until the expiration of the user cert
        in .globus/usercert.pem or $X509_USER_CERT if set
        """
        cmd = 'openssl x509 -noout -dates -in %s' % self.certLocation
        stdout, stderr, rc = execute_command(cmd, logger=self.logger)
        if rc != 0:
            self.logger.error(stdout+'\n'+stderr)
            msg = "\n".join(['Error executing %s:' % cmd, stdout, stderr])
            raise ProxyCreationException(msg)
        out = stdout.rstrip().split('notAfter=')[1]

        possibleFormats = ['%b  %d  %H:%M:%S %Y %Z',
                           '%b %d %H:%M:%S %Y %Z']
        exptime = None
        for frmt in possibleFormats:
            try:
                exptime = datetime.strptime(out, frmt)
            except ValueError:
                pass  # try next format
        if not exptime:
            # If we cannot decode the output in any way print
            # a message and fallback to voms-proxy-info command
            self.logger.warning(
                'Cannot decode "openssl x509 -noout -in %s -dates" date format.' % self.certLocation)
            timeleft = 0
        else:
            # if everything is fine then we are ready to return!!
            timeleft = (exptime - datetime.utcnow()).total_seconds()
        daystoexp = int(timeleft // (60. * 60 * 24))
        return daystoexp

