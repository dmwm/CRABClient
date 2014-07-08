import subprocess
from os import path, remove
import datetime

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_utilities import colors
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from CRABClient.client_exceptions import MissingOptionException,ConfigurationException
from httplib import HTTPException

class checkwrite(SubCommand):
    """
      let user to test if he/she have permission to write on specify site
    """
    name = 'checkwrite'
    shortnames = ['chk']


    def __call__(self):

        self.filename = 'crab3checkwrite.tmp'

        self.username = self.proxy.getHyperNewsName()
        phedex = PhEDEx({"cert": self.proxyfilename, "key": self.proxyfilename})

        if hasattr(self.options, 'userlfn') and self.options.userlfn != None:
            lfnsadd = self.options.userlfn + '/' + self.filename
        else:
            lfnsadd = '/store/user/' + self.username + '/' + self.filename

        try:
            pfndict = phedex.getPFN(nodes = [self.options.sitename], lfns = [lfnsadd])
            pfn = pfndict[(self.options.sitename, lfnsadd)]
            if not pfn:
                self.logger.info('%sError%s: Failed to get PFN from the site. Please check the site status' % (colors.RED, colors.NORMAL))
                raise ConfigurationException
        except HTTPException, errormsg :
            self.logger.info('%sError:%s Failed to contact PhEDEx or wrong PhEDEx node name is used' % (colors.RED, colors.NORMAL))
            self.logger.info('Result: %s\nStatus :%s\nURL :%s' % (errormsg.result, errormsg.status, errormsg.url))
            raise HTTPException, errormsg

        self.logger.info('Attempting to copy file %s to site %s' % (self.filename, self.options.sitename))
        cpout, cperr, cpexitcode = self.lcgcp(pfn)

        if cpexitcode == 0:
            self.logger.info('Successfully copied file %s to site %s' % (self.filename, self.options.sitename))
            self.logger.info('Attempting to delete file %s from site %s' % (pfn, self.options.sitename))
            delexitcode = self.lcgdelete(pfn)
            if delexitcode:
                self.logger.info('Warning: Failed to delete file %s from site %s' % (pfn, self.options.sitename))
            else:
                self.logger.info('Successfully deleted file %s from site %s' % (pfn, self.options.sitename))
            exitcode = 0
        elif 'Permission denied' in cperr or 'mkdir: cannot create directory' in cperr:
            exitcode = 1
        elif 'timeout' in cpout or 'timeout' in cperr:
            self.logger.info("%sError: %sConnection time out, try again later" %(colors.RED, colors.NORMAL))
            exitcode = -1
        elif 'exist' in cpout or 'exist' in cperr:
            self.logger.info('Error copying file %s to site %s, it may be that file already exists' % (self.filename, self.options.sitename))
            self.logger.info('Attempting to delete file %s from site %s' % (pfn, self.options.sitename))
            delexitcode = self.lcgdelete(pfn)
            if delexitcode:
                self.logger.info('Failed to delete file %s from site %s' % (pfn, self.options.sitename))
                exitcode = -1
            else:
                self.logger.info('Successfully deleted file %s from site %s' % (pfn, self.options.sitename))
                self.logger.info('Attempting to copy file %s to site %s again' % (self.filename, self.options.sitename))
                cpout, cperr, cpexitcode = self.lcgcp(pfn)
                if cpexitcode == 0:
                    self.logger.info('Successfully copied file %s to site %s' % (self.filename, self.options.sitename))
                    self.logger.info('Attempting to delete file %s from site %s' % (pfn, self.options.sitename))
                    delexitcode = self.lcgdelete(pfn)
                    if delexitcode:
                        self.logger.info('Failed to delete file %s from site %s' % (pfn, self.options.sitename))
                    else:
                        self.logger.info('Successfully deleted file %s from site %s' % (pfn, self.options.sitename))
                    exitcode = 0
                else:
                    exitcode = 1
        else:
            exitcode = 1

        if exitcode == 0:
            self.logger.info('%sSuccess%s: Able to write on site %s' % (colors.GREEN, colors.NORMAL, self.options.sitename))
        elif exitcode == -1:
            self.logger.info('Unable to check write permission on site %s' % self.options.sitename)
        else:
            self.logger.info('%sError%s: Unable to write on site %s' % (colors.RED, colors.NORMAL, self.options.sitename))


    def lcgcp(self, pfn):

        try:
            with open(self.filename, 'w') as file:
                file.write('This is a dummy file created by the crab checkwrite command on %s' % str(datetime.datetime.now().strftime('%d/%m/%Y at %H:%M:%S')))
        except IOError:
            self.logger.info('%sError%s: Failed to create file %s' % (colors.RED, colors.NORMAL, self.filename))
            raise Exception

        abspath = path.abspath(self.filename)

        cpcmd = 'lcg-cp -v -b -D srmv2 --connect-timeout 180 ' + abspath + ' ' + pfn
        self.logger.info('Executing command: %s' % cpcmd)
        self.logger.info('Please wait...')
        cpprocess = subprocess.Popen(cpcmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        cpout, cperr = cpprocess.communicate()
        cpexitcode = cpprocess.returncode

        if cpexitcode:
            self.logger.info('Failed running lcg-cp')
            if cpout:
                self.logger.info('  Stdout:\n    %s' % cpout.replace('\n','\n    '))
            if cperr:
                self.logger.info('  Stderr:\n    %s' % cperr.replace('\n','\n    '))
        else:
            self.logger.info('Successfully ran lcg-cp')

        try:
            remove(abspath)
        except Exception:
            self.logger.info('%sError%s: Failed to delete file %s' % (colors.RED, colors.NORMAL, self.filename))
            pass

        return cpout, cperr, cpexitcode


    def lcgdelete(self, pfn):

        rmcmd = 'lcg-del --connect-timeout 180 -b -l -D srmv2 ' + pfn
        self.logger.info('Executing command: %s' % rmcmd)
        self.logger.info('Please wait...')
        delprocess = subprocess.Popen(rmcmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        delout, delerr = delprocess.communicate()
        delexitcode = delprocess.returncode

        if delexitcode:
            self.logger.info('Failed running lcg-del')
            if delout:
                self.logger.info('  Stdout:\n    %s' % delout.replace('\n','\n    '))
            if delerr:
                self.logger.info('  Stderr:\n    %s' % delerr.replace('\n','\n    '))
        else:
            self.logger.info('Successfully ran lcg-del')

        return delexitcode


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '--site',
                                dest = 'sitename',
                                help = 'The PhEDEx node name of the site to be checked.')
        self.parser.add_option( '--lfn',
                                dest = 'userlfn',
                                help = 'A user lfn address.')


    def validateOptions(self):
        SubCommand.validateOptions(self)

        if not hasattr(self.options, 'sitename') or self.options.sitename is None:
            self.logger.info("Missing site name, use '--site' option")
            raise MissingOptionException
