import subprocess
from os import path , remove

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_utilities import colors
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from CRABClient.client_exceptions import MissingOptionException,ConfigurationException
from httplib import HTTPException

class checkwrite(SubCommand):
    """
      let user to test if he/she have permission to write on specify site
    """
    name ='checkwrite'
    shortnames = ['chk']


    def __call__(self):

        self.filename ='crab3chkwrite.tmp'
        self.username = self.proxy.getHyperNewsName()
        phedex = PhEDEx({"cert":self.proxyfilename, "key":self.proxyfilename})

        if hasattr(self.options, 'userlfn') and self.options.userlfn != None:
            lfnsadd = self.options.userlfn +'/'+ self.filename
        else:
            lfnsadd = '/store/user/'+self.username+'/'+self.filename

        try:
            pfndict=phedex.getPFN(nodes = [self.options.sitename], lfns = [lfnsadd])
            pfn = pfndict[(self.options.sitename,lfnsadd)]
            if pfn == None:
                self.logger.info('%sError%s: Failed to get pfn from the site, Please check site status' % (colors.RED, colors.NORMAL) )
                raise ConfigurationException
        except HTTPException, errormsg :
            self.logger.info('%sError%s: Failed to contact PhEDEx or wrong PhEDEx node name is use' %(colors.RED, colors.NORMAL))
            self.logger.info('Result: %s\nStatus: %s\nurl: %s' %(errormsg.result, errormsg.status, errormsg.url))
            raise HTTPException, errormsg

        cpout, cperr, cpexitcode = self.lcgcp(pfn)

        if cpexitcode == 0:
            delexitcode=self.lcgdelete(pfn)
            exitcode = 0

        elif 'timeout' in cpout or 'timeout' in cperr:
            self.logger.info("%sError%s: Connection time out, try again later" %(colors.RED, colors.NORMAL))
            exitcode = 1
        elif 'exist' in cpout or 'exist' in cperr:
            exitcode = 1
            self.logger.info('Attempting to delete %s on site' % self.filename)
            delexitcode=self.lcgdelete(pfn)
            if delexitcode == 0:
                self.logger.info('Attempting to write on site again')
                cpout, cperr, cpexitcode=self.lcgcp(pfn)

                if cpexitcode == 0:
                    delexitcode=self.lcgdelete(pfn)
                    exitcode = 0
        else:
            exitcode = 1

        if exitcode == 0:
            self.logger.info("%sSuccess%s: Successfully write on site %s" %(colors.GREEN, colors.NORMAL, self.options.sitename))
        elif exitcode != 0:
            self.logger.info("%sError%s: Unable to write on site %s" % (colors.RED, colors.NORMAL, self.options.sitename))




    def lcgcp(self, pfn ):

        try:
            file = open(self.filename,'w')
            file.close()
        except IOError:
            self.logger.info("%sError%s:  failed to create local %s" % (colors.RED,colors.NORMAL,self.filename))
            raise Exception

        abspath=path.abspath(self.filename)

        cpcmd ="lcg-cp -v -b -D srmv2 --connect-timeout 180 " + abspath +' '+ pfn
        self.logger.info("Attempting to write on site: %s \nExecuting the command: %s\nPlease wait" %(self.options.sitename, cpcmd))
        cpprocess = subprocess.Popen(cpcmd, stdout= subprocess.PIPE, stderr= subprocess.PIPE, shell= True)
        cpout , cperr = cpprocess.communicate()
        cpexitcode = cpprocess.returncode

        if cpexitcode != 0 :
            self.logger.info("%sError%s: Error in lcg-cp \nStdout: \n%s\nStderr: \n%s" %(colors.RED,colors.NORMAL,cpout,cperr))
        elif cpexitcode == 0 :
            self.logger.info("%sSuccess%s: Successfully run lcg-cp" %(colors.GREEN, colors.NORMAL))

        try:
            remove(abspath)
        except Exception:
            self.logger.info("%sError%s: Failed in deleting local %s" % self.filename)
            pass

        return cpout, cperr, cpexitcode

    def lcgdelete(self,pfn):

        self.logger.info("Deleting file: %s" %pfn)
        rmcmd ="lcg-del --connect-timeout 180 -b  -l -D srmv2 "+pfn
        self.logger.info("Executing command: %s" % rmcmd)
        delprocess = subprocess.Popen(rmcmd, stdout= subprocess.PIPE, stderr= subprocess.PIPE, shell=True)
        delout, delerr = delprocess.communicate()
        delexitcode = delprocess.returncode

        if delexitcode != 0:
            self.logger.info("%sError%s:  Failed in running lcg-del\nStdout:\n%s\nStderr:\n%s" \
                             % (colors.RED, colors.NORMAL, delout, delerr))
        elif delexitcode == 0 :
            self.logger.info("%sSuccess%s: Successfully run lcg-del" %(colors.GREEN, colors.NORMAL))

        return delexitcode
#return exit code

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '--site',
                                dest = 'sitename',
                                help = 'The PhEDEx node name of site to be check.')

        self.parser.add_option( '--lfn',
                                dest = 'userlfn',
                                help = 'A user lfn address.')

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if not hasattr(self.options,'sitename') or self.options.sitename is None:
            self.logger.info("Missing site name, use '--site' options")
            raise MissingOptionException
