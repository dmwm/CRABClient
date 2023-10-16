import os
import re
import time
import datetime

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import execute_command, colors, cmd_exist
from CRABClient.UserUtilities import getUsername
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException
from ServerUtilities import checkOutLFN

class checkwrite(SubCommand):
    """
    Let user to test if he/she have permission to write on specify site
    """
    name = 'checkwrite'
    shortnames = ['chk']

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        self.filename = None
        self.subdir = None
        self.lfnPrefix = None


    def __call__(self):

        username = getUsername(self.proxyfilename, logger=self.logger)
        if hasattr(self.options, 'userlfn') and self.options.userlfn != None:
            self.lfnPrefix = self.options.userlfn
        else:
            ## If the user didn't provide an LFN path where to check the write permission,
            ## assume he/she wants to check in /store/user/<username>
            self.logger.info('Will check write permission in the default location /store/user/<username>')
            self.lfnPrefix = '/store/user/' + username

        ## Check that the location where we want to check write permission
        ## is one where the user will be allowed to stageout.
        self.logger.info("Validating LFN %s...", self.lfnPrefix)
        # if an error message is needed later, prepare it now to keep code below tidy
        errMsg  = "Refusing to check write permission in %s, because this is not an allowed LFN for stageout." % (self.lfnPrefix)
        errMsg += "\nThe LFN must start with either"
        errMsg += " '/store/user/<username>/' or '/store/group/<groupname>/'"
        errMsg += " (or '/store/local/<something>/' if publication is off),"
        errMsg += " where username is your username as registered in CMS"
        errMsg += " (i.e. the username of your CERN primary account)."
        errMsg += "\nLFN %s is not valid." % (self.lfnPrefix)

        if not checkOutLFN(self.lfnPrefix, username):
            self.logger.info(errMsg)
            return {'status': 'FAILED'}
        else:
            self.logger.info("LFN %s is valid.", self.lfnPrefix)

        # we need Rucio to check LFN to PFN, but it does not exist in CC6 singularity image
        # where there is no python3. And in any case Rucio will not support python2 in the future
        if not cmd_exist("python3"):
            self.logger.info("No python3. Not possible to use Rucio in this environment")
            return {'status': 'FAILED'}

        cp_cmd = ""
        if cmd_exist("gfal-copy") and cmd_exist("gfal-rm") and self.command in [None, "GFAL"]:
            self.logger.info("Will use `gfal-copy`, `gfal-rm` commands for checking write permissions")
            cp_cmd = "gfal-copy -p -v -t 180 "
            delfile_cmd = "gfal-rm -v -t 180 "
            deldir_cmd = "gfal-rm -r -v -t 180 "
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

        self.logger.info('Will check write permission in %s on site %s', self.lfnPrefix, self.options.sitename)
        timestamp = str(time.strftime("%Y%m%d_%H%M%S"))
        self.filename = 'crab3checkwrite_' + timestamp  + '.tmp'
        self.subdir = 'crab3checkwrite_' + timestamp
        lfn = self.lfnPrefix + '/' + self.subdir + '/' + self.filename
        site = self.options.sitename
        pfn = self.getPFN(site=site, lfn=lfn, username=username)
        self.createFile()
        self.logger.info("Will use PFN: %s", pfn)
        dirpfn = pfn[:len(pfn)-len(self.filename)]
        self.logger.info('\nAttempting to create (dummy) directory %s and copy (dummy) file %s to %s\n' % (self.subdir, self.filename, self.lfnPrefix))
        cpout, cperr, cpexitcode = self.cp(pfn, cp_cmd)
        if cpexitcode == 0:
            self.logger.info('\nSuccessfully created directory %s and copied file %s to %s' % (self.subdir, self.filename, self.lfnPrefix))
            self.logger.info('\nAttempting to delete file %s\n' % (pfn))
            delexitcode = self.delete(pfn, delfile_cmd)
            if delexitcode:
                self.logger.info('\nFailed to delete file %s' % (pfn))
                finalmsg  = '%sError%s: CRAB3 is able to copy but unable to delete file in %s on site %s. Asynchronous Stage Out with CRAB3 will fail.' % (colors.RED, colors.NORMAL, self.lfnPrefix, self.options.sitename)
                finalmsg += '\n       You may want to contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                returndict = {'status': 'FAILED'}
            else:
                self.logger.info('\nSuccessfully deleted file %s' % (pfn))
                self.logger.info('\nAttempting to delete directory %s\n' % (dirpfn))
                delexitcode = self.delete(dirpfn, deldir_cmd)
                if delexitcode:
                    self.logger.info('\nFailed to delete directory %s' % (dirpfn))
                    finalmsg  = '%sError%s: CRAB3 is able to copy but unable to delete directory in %s on site %s. Asynchronous Stage Out with CRAB3 will fail.' % (colors.RED, colors.NORMAL, self.lfnPrefix, self.options.sitename)
                    finalmsg += '\n       You may want to contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                    returndict = {'status': 'FAILED'}
                else:
                    self.logger.info('\nSuccessfully deleted directory %s' % (dirpfn))
                    finalmsg = '%sSuccess%s: Able to write in %s on site %s' % (colors.GREEN, colors.NORMAL, self.lfnPrefix, self.options.sitename)
                    returndict = {'status': 'SUCCESS'}
        else:
            if 'Permission denied' in cperr or 'mkdir: cannot create directory' in cperr:
                finalmsg  = '%sError%s: Unable to write in %s on site %s' % (colors.RED, colors.NORMAL, self.lfnPrefix, self.options.sitename)
                finalmsg += '\n       You may want to contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                returndict = {'status': 'FAILED'}
            elif 'timeout' in cpout or 'timeout' in cperr:
                self.logger.info('Connection time out.')
                finalmsg  = '\nUnable to check write permission in %s on site %s' % (self.lfnPrefix, self.options.sitename)
                finalmsg += '\nPlease try again later or contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                returndict = {'status': 'FAILED'}
            else:
                finalmsg  = 'Unable to check write permission in %s on site %s' % (self.lfnPrefix, self.options.sitename)
                finalmsg += '\nPlease try again later or contact the site administrators sending them the \'crab checkwrite\' output as printed above.'
                returndict = {'status' : 'FAILED'}
        self.removeFile()

        self.logger.info('\nCheckwrite Result:')
        self.logger.info(finalmsg)
        if returndict['status'] == 'FAILED':
            self.logger.info('%sNote%s: You cannot write to a site if you did not ask permission.' % (colors.BOLD, colors.NORMAL))
            if 'CH_CERN' in self.options.sitename:
                dbgmsg = '%sAdditional diagnostic info for CERN EOS%s\n' % (colors.RED, colors.NORMAL)
                dbgcmd = "echo '== id ==>:';id"
                dbgcmd += ";echo '== voms-proxy-info -all ==>:';voms-proxy-info -all"
                dbgcmd += ";which uberftp > /dev/null 2>&1 && echo '== uberftp eoscmsftp.cern.ch pwd ==>:'"
                dbgcmd += ";which uberftp > /dev/null 2>&1 && uberftp eoscmsftp.cern.ch pwd"
                dbgcmd += ";which uberftp > /dev/null 2>&1 || echo 'WARNING uberftp command not found. To get additional diagnostic info'"
                dbgcmd += ";which uberftp > /dev/null 2>&1 || echo ' log on lxplus, get a proxy and execute:'"
                dbgcmd += ";which uberftp > /dev/null 2>&1 || echo ' uberftp eoscmsftp.cern.ch pwd'"
                #self.logger.info('Executing command: %s' % cmd)
                #self.logger.info('Please wait...')
                output, _, _ = execute_command(command=dbgcmd)
                dbgmsg += output
                self.logger.info(dbgmsg)
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
        except Exception:
            self.logger.info('%sWarning%s: Failed to delete file %s' % (colors.RED, colors.NORMAL, self.filename))


    def getPFN(self, site='T2_CH_CERN', lfn='/store/user', username='jdoe'):

        # prepare a simply python script to resolve lfn2pfn via Rucio
        template = """
from rucio.client import Client
client = Client()
rse = "{site}"
lfn = ["user.{username}:{lfn}"]
for operation in ['third_party_copy_write', 'write', 'read']:
    try:
        #print('Try Rucio lfn2pn with operation %s', operation)
        out = client.lfns2pfns(rse, lfn, operation=operation)
        break
    except Exception as ex:
        print("Failed to resolve LNF to PFN via Rucio. Error is:\\n %s" % str(ex))
if not out:
    print("Failed to resolve LNF to PFN via Rucio.")
    exit(1)
print(out[lfn[0]])
exit(0)
"""
        rucioScript = template.format(site=site, username=username, lfn=lfn)
        import tempfile
        (_, scriptName) = tempfile.mkstemp(dir='/tmp', prefix='crab_lfn2pfn-', suffix='.py')
        with open(scriptName, 'w') as ofile:
            ofile.write(rucioScript)
        cmd = 'eval `scram unsetenv -sh`; '
        cmd += 'source /cvmfs/cms.cern.ch/rucio/setup-py3.sh >/dev/null; '
        cmd += 'export RUCIO_ACCOUNT=%s; ' % username
        cmd += 'python3 %s; ' % scriptName
        rucioOut, rucioErr, exitcode = execute_command(cmd)
        os.unlink(scriptName)
        if exitcode:
            self.logger.info('PFN lookup failed')
            if rucioOut:
                self.logger.info('  Stdout:\n    %s' % str(rucioOut).replace('\n', '\n    '))
            if rucioErr:
                self.logger.info('  Stderr:\n    %s' % str(rucioErr).replace('\n', '\n    '))
            raise Exception
        pfn = rucioOut.rstrip()
        return pfn


    def cp(self, pfn, command):

        abspath = os.path.abspath(self.filename)
        if cmd_exist("gfal-copy")  and self.command in [None, "GFAL"]:
            abspath = "file://" + abspath
        undoScram = "which scram >/dev/null 2>&1 && eval `scram unsetenv -sh`"
        cpcmd = undoScram + "; " + command + abspath + " '" + pfn + "'"
        self.logger.info('Executing command: %s' % cpcmd)
        self.logger.info('Please wait...')
        cpout, cperr, cpexitcode = execute_command(cpcmd)
        if cpexitcode:
            self.logger.info('Failed running copy command')
            if cpout:
                self.logger.info('  Stdout:\n    %s' % str(cpout).replace('\n', '\n    '))
            if cperr:
                self.logger.info('  Stderr:\n    %s' % str(cperr).replace('\n', '\n    '))

        return cpout, cperr, cpexitcode


    def delete(self, pfn, command):

        undoScram = "which scram >/dev/null 2>&1 && eval `scram unsetenv -sh`"
        rmcmd = undoScram + "; " + command + "'" + pfn + "'"
        self.logger.info('Executing command: %s' % rmcmd)
        self.logger.info('Please wait...')
        delout, delerr, delexitcode = execute_command(rmcmd)
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
                               dest='sitename',
                               default=None,
                               help='The PhEDEx node name of the site to be checked.')
        self.parser.add_option('--lfn',
                               dest='userlfn',
                               default=None,
                               help='A user lfn address.')
        self.parser.add_option('--checksum',
                               dest='checksum',
                               default='no',
                               help='Set it to yes if needed. It will use ADLER32 checksum' +\
                                       'Allowed values are yes/no. Default is no.')
        self.parser.add_option('--command',
                               dest='command',
                               default=None,
                               help='A command which to use. Available commands are LCG or GFAL.')


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
