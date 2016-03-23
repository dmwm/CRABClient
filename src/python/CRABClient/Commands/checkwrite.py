import os
import re
import time
import datetime
import subprocess

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import colors, getUserDNandUsernameFromSiteDB, cmd_exist
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException
from httplib import HTTPException
from ServerUtilities import checkOutLFN

class checkwrite(SubCommand):
    """
    Let user to test if he/she have permission to write on specify site
    """
    name = 'checkwrite'
    shortnames = ['chk']

    def __init__(self, logger, cmdargs = None):
        SubCommand.__init__(self, logger, cmdargs)
        self.phedex = PhEDEx({"cert": self.proxyfilename, "key": self.proxyfilename, "logger": self.logger, "pycurl" : True})
        self.lfnsaddprefix = None
        self.filename = None


    def __call__(self):
        username = None
        if hasattr(self.options, 'userlfn') and self.options.userlfn != None:
            self.lfnsaddprefix = self.options.userlfn
        else:
            ## If the user didn't provide an LFN path where to check the write permission,
            ## assume he/she wants to check in /store/user/<username>. Retrieve his/her
            ## username from SiteDB.
            self.logger.info('Will check write permission in the default location /store/user/<username>')
            username = getUserDNandUsernameFromSiteDB(self.logger).get('username')
            if username:
                self.lfnsaddprefix = '/store/user/' + username
            else:
                return {'status': 'FAILED'}

        ## Check that the location where we want to check write permission
        ## is one where the user will be allowed to stageout.
        self.logger.info("Validating LFN %s..." % (self.lfnsaddprefix))
        msg  = "Refusing to check write permission in %s, because this is not an allowed LFN for stageout." % (self.lfnsaddprefix)
        msg += "\nThe LFN must start with either"
        msg += " '/store/user/<username>/' or '/store/group/<groupname>/'"
        msg += " (or '/store/local/<something>/' if publication is off),"
        msg += " where username is your username as registered in SiteDB"
        msg += " (i.e. the username of your CERN primary account)."
        msg += "\nLFN %s is not valid." % (self.lfnsaddprefix)
        if not username and self.lfnsaddprefix.startswith('/store/user/'):
            username = getUserDNandUsernameFromSiteDB(self.logger).get('username')
        if not checkOutLFN(self.lfnsaddprefix, username):
            self.logger.info(msg)
            return {'status': 'FAILED'}
        else:
            self.logger.info("LFN %s is valid." % (self.lfnsaddprefix))

        cp_cmd = ""
        if cmd_exist("gfal-copy") and cmd_exist("gfal-rm") and self.command in [None, "GFAL"]:
            self.logger.info("Will use `gfal-copy`, `gfal-rm` commands for checking write permissions")
            cp_cmd = "env -i X509_USER_PROXY=%s gfal-copy -p -v -t 180 " % os.path.abspath(self.proxyfilename)
            delfile_cmd = "env -i X509_USER_PROXY=%s gfal-rm -v -t 180 " % os.path.abspath(self.proxyfilename)
            deldir_cmd = "env -i X509_USER_PROXY=%s gfal-rm -r -v -t 180 " % os.path.abspath(self.proxyfilename)
            if self.checksum:
                cp_cmd += "-K %s " % self.checksum
        elif cmd_exist("lcg-cp") and cmd_exist("lcg-del"):
            self.logger.info("Will use `lcg-cp`, `lcg-del` commands for checking write permissions")
            cp_cmd = "lcg-cp -v -b -D srmv2 --connect-timeout 180 "
            delfile_cmd = "lcg-del --connect-timeout 180 -b -l -D srmv2 "
            deldir_cmd = "lcg-del -d --connect-timeout 180 -b -l -D srmv2 "
            if self.checksum:
                cp_cmd += "--checksum-type %s " % self.checksum
        else:
            self.logger.info("Neither gfal nor lcg command was found")
            return {'status': 'FAILED'}


        self.logger.info('Will check write permission in %s on site %s' % (self.lfnsaddprefix, self.options.sitename))
        timestamp =  str(time.strftime("%Y%m%d_%H%M%S"))
        self.filename = 'crab3checkwrite_' + timestamp  + '.tmp'
        self.subdir = 'crab3checkwrite_' + timestamp
        self.createFile()
        pfn = self.getPFN()
        dirpfn = pfn[:len(pfn)-len(self.filename)]
        self.logger.info('\nAttempting to create (dummy) directory %s and copy (dummy) file %s to %s\n' % (self.subdir, self.filename, self.lfnsaddprefix))
        cpout, cperr, cpexitcode = self.cp(pfn, cp_cmd)
        if cpexitcode == 0:
            self.logger.info('\nSuccessfully created directory %s and copied file %s to %s' % (self.subdir, self.filename, self.lfnsaddprefix))
            self.logger.info('\nAttempting to delete file %s\n' % (pfn))
            delexitcode = self.delete(pfn, delfile_cmd)
            if delexitcode:
                self.logger.info('\nFailed to delete file %s' % (pfn))
                finalmsg  = '%sError%s: CRAB3 is able to copy but unable to delete file in %s on site %s. Asynchronous Stage Out with CRAB3 will fail.' % (colors.RED, colors.NORMAL, self.lfnsaddprefix, self.options.sitename)
                finalmsg += '\n       You may want to contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                returndict = {'status': 'FAILED'}
            else:
                self.logger.info('\nSuccessfully deleted file %s' % (pfn))
                self.logger.info('\nAttempting to delete directory %s\n' % (dirpfn))
                delexitcode = self.delete(dirpfn, deldir_cmd)
                if delexitcode:
                    self.logger.info('\nFailed to delete directory %s' % (dirpfn))
                    finalmsg  = '%sError%s: CRAB3 is able to copy but unable to delete directory in %s on site %s. Asynchronous Stage Out with CRAB3 will fail.' % (colors.RED, colors.NORMAL, self.lfnsaddprefix, self.options.sitename)
                    finalmsg += '\n       You may want to contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                    returndict = {'status': 'FAILED'}
                else:
                    self.logger.info('\nSuccessfully deleted directory %s' % (dirpfn))
                    finalmsg = '%sSuccess%s: Able to write in %s on site %s' % (colors.GREEN, colors.NORMAL, self.lfnsaddprefix, self.options.sitename)
                    returndict = {'status': 'SUCCESS'}
        else:
            if 'Permission denied' in cperr or 'mkdir: cannot create directory' in cperr:
                finalmsg  = '%sError%s: Unable to write in %s on site %s' % (colors.RED, colors.NORMAL, self.lfnsaddprefix, self.options.sitename)
                finalmsg += '\n       You may want to contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                returndict = {'status': 'FAILED'}
            elif 'timeout' in cpout or 'timeout' in cperr:
                self.logger.info('Connection time out.')
                finalmsg  = '\nUnable to check write permission in %s on site %s' % (self.lfnsaddprefix, self.options.sitename)
                finalmsg += '\nPlease try again later or contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                returndict = {'status': 'FAILED'}
            else:
                finalmsg  = 'Unable to check write permission in %s on site %s' % (self.lfnsaddprefix, self.options.sitename)
                finalmsg += '\nPlease try again later or contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                returndict = {'status' : 'FAILED'}
        self.removeFile()

        self.logger.info('\nCheckwrite Result:')
        self.logger.info(finalmsg)
        if returndict['status'] == 'FAILED':
            self.logger.info('%sNote%s: You cannot write to a site if you did not ask permission.' % (colors.BOLD, colors.NORMAL))

        return returndict


    def createFile(self):

        abspath = os.path.abspath(self.filename)
        try:
            with open(abspath, 'w') as fd:
                fd.write('This is a dummy file created by the crab checkwrite command on %s' % str(datetime.datetime.now().strftime('%d/%m/%Y at %H:%M:%S')))
        except IOError:
            self.logger.info('%sError%s: Failed to create file %s' % (colors.RED, colors.NORMAL, self.filename))
            raise Exception


    def removeFile(self):

        abspath = os.path.abspath(self.filename)
        try:
            os.remove(abspath)
        except:
            self.logger.info('%sWarning%s: Failed to delete file %s' % (colors.RED, colors.NORMAL, self.filename))


    def getPFN(self):

        lfnsadd = self.lfnsaddprefix + '/' + self.subdir + '/' + self.filename
        try:
            pfndict = self.phedex.getPFN(nodes = [self.options.sitename], lfns = [lfnsadd])
            pfn = pfndict[(self.options.sitename, lfnsadd)]
            if not pfn:
                self.logger.info('%sError%s: Failed to get PFN from the site. Please check the site status' % (colors.RED, colors.NORMAL))
                raise ConfigurationException
        except HTTPException as errormsg:
            self.logger.info('%sError%s: Failed to contact PhEDEx or wrong PhEDEx node name is used' % (colors.RED, colors.NORMAL))
            self.logger.info('Result: %s\nStatus :%s\nURL :%s' % (errormsg.result, errormsg.status, errormsg.url))
            raise

        return pfn


    def cp(self, pfn, command):

        abspath = os.path.abspath(self.filename)
        if cmd_exist("gfal-copy")  and self.command in [None, "GFAL"]:
            abspath = "file://" + abspath
        cpcmd = command + abspath + " '" + pfn + "'"
        self.logger.info('Executing command: %s' % cpcmd)
        self.logger.info('Please wait...')
        cpprocess = subprocess.Popen(cpcmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        cpout, cperr = cpprocess.communicate()
        cpexitcode = cpprocess.returncode
        if cpexitcode:
            self.logger.info('Failed running copy command')
            if cpout:
                self.logger.info('  Stdout:\n    %s' % str(cpout).replace('\n', '\n    '))
            if cperr:
                self.logger.info('  Stderr:\n    %s' % str(cperr).replace('\n', '\n    '))

        return cpout, cperr, cpexitcode


    def delete(self, pfn, command):

        rmcmd = command + "'" + pfn + "'"
        self.logger.info('Executing command: %s' % rmcmd)
        self.logger.info('Please wait...')
        delprocess = subprocess.Popen(rmcmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        delout, delerr = delprocess.communicate()
        delexitcode = delprocess.returncode
        if delexitcode:
            self.logger.info('Failed running delete command')
            if delout:
                self.logger.info('  Stdout:\n    %s' % str(delout).replace('\n', '\n    '))
            if delerr:
                self.logger.info('  Stderr:\n    %s' % str(delerr).replace('\n', '\n    '))

        return delexitcode


    def terminate(self, exitcode):
        pass


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--site',
                               dest = 'sitename',
                               default = None,
                               help = 'The PhEDEx node name of the site to be checked.')
        self.parser.add_option('--lfn',
                               dest = 'userlfn',
                               default = None,
                               help = 'A user lfn address.')
        self.parser.add_option('--checksum',
                               dest = 'checksum',
                               default = 'yes',
                               help = 'Set it to true if needed. If true will use ADLER32 checksum' +\
                                       'Allowed values are yes/no. Default is yes.')
        self.parser.add_option('--command',
                               dest = 'command',
                               default = None,
                               help = 'A command which to use. Available commands are LCG or GFAL.')


    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.sitename is None:
            msg  = "%sError%s: Please specify the site where to check the write permissions." % (colors.RED, colors.NORMAL)
            msg += " Use the --site option."
            ex = MissingOptionException(msg)
            ex.missingOption = "sitename"
            raise ex
        if hasattr(self.options, 'command') and self.options.command != None:
            AvailableCommands = ['LCG', 'GFAL']
            self.command = self.options.command.upper()
            if self.command not in AvailableCommands:
                msg = "You specified to use %s command and it is not allowed. Available commands are: %s " % (self.command, str(AvailableCommands))
                ex = ConfigurationException(msg)
                raise ex
        else:
            self.command = None
        if hasattr(self.options, 'checksum'):
            if re.match('^yes$|^no$', self.options.checksum):
                self.checksum = 'ADLER32' if self.options.checksum == 'yes' else None
            else:
                msg = "You specified to use %s checksum. Only lowercase yes/no is accepted to turn ADLER32 checksum" % self.options.checksum
                ex = ConfigurationException(msg)
                raise ex
