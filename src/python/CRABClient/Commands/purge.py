from WMCore.Services.UserFileCache.UserFileCache  import UserFileCache
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_utilities import colors
from CRABClient.client_utilities import server_info
from CRABClient.client_exceptions import ConfigurationException, MissingOptionException, RESTCommunicationException
from httplib import HTTPException
from RESTInteractions import HTTPRequests
from CRABClient import __version__

import glob
import subprocess

class purge(SubCommand):
    """
    clean user schedd and cache for a given workflow
    """

    visible = True

    def __call__(self):

        self.logger.info('Getting the tarball hash key')

        tarballdir=glob.glob(self.requestarea+'/inputs/*.tgz')
        if len(tarballdir) != 1 :
            self.logger.info('%sError %s:Could not found tarball or there is more than one tarball'% (colors.RED, colors.NORMAL))
            raise ConfigurationException
        tarballdir=tarballdir[0]

        #checking task status

        self.logger.info('Checking task status')
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
        dictresult, status, _ = server.get(self.uri, data = { 'workflow' : self.cachedinfo['RequestName'], 'verbose': 0 })

        dictresult = dictresult['result'][0] #take just the significant part

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.info('Task status: %s' % dictresult['status'])
        accepstate = ['KILLED','FINISHED','FAILED','KILLFAILED', 'COMPLETED']
        if dictresult['status'] not in accepstate:
            msg = ('%sERROR %s: Only task with this status can be purge: {0}'.format(accepstate) % (colors.RED, colors.NORMAL))
            raise ConfigurationException(msg)

        #getting the cache url

        if not self.options.scheddonly:
            baseurl=self.getUrl(self.instance, resource='info')
            cacheurl=server_info('backendurls', self.serverurl, self.proxyfilename, baseurl)
            cacheurl=cacheurl['cacheSSL']
            cacheurldict={'endpoint' : cacheurl, 'pycurl': True}

            ufc = UserFileCache(cacheurldict)
            hashkey = ufc.checksum(tarballdir)
            self.logger.info('Tarball hashkey :%s' %hashkey)
            self.logger.info('Attempting to clean user file cache')
            ufcresult = ufc.removeFile(hashkey)
            if ufcresult == '' :
                self.logger.info('%sSuccess %s:Successfully remove file from cache' % (colors.GREEN, colors.NORMAL))
            else:
                self.logger.info('%sError %s:Failed to remove the file from cache' % (colors.RED, colors.NORMAL))

        if not self.options.cacheonly:
            self.logger.info('Getting the schedd address')
            baseurl=self.getUrl(self.instance, resource='info')
            try:
                sceddaddress = server_info('scheddaddress', self.serverurl, self.proxyfilename, baseurl, workflow = self.cachedinfo['RequestName'] )
            except HTTPException, he:
                self.logger.info('%sError %s:Failed to get the schedd address' % (colors.RED, colors.NORMAL))
                raise HTTPException,he
            self.logger.debug('%sSuccess %s:Successfully getting schedd address' % (colors.GREEN, colors.NORMAL))
            self.logger.debug('Schedd address: %s' % sceddaddress)
            self.logger.info('Attempting to clean user file schedd')

            gssishrm = 'gsissh -o ConnectTimeout=60 -o PasswordAuthentication=no ' + sceddaddress + ' rm -rf ' + self.cachedinfo['RequestName']
            self.logger.debug('gsissh command: %s' % gssishrm)

            delprocess=subprocess.Popen(gssishrm, stdout= subprocess.PIPE, stderr= subprocess.PIPE, shell=True)
            stdout, stderr = delprocess.communicate()
            exitcode = delprocess.returncode

            if exitcode == 0 :
                self.logger.info('%sSuccess %s:Successfully remove task from scehdd' % (colors.GREEN, colors.NORMAL))
            else :
                self.logger.info('%sError %s:Failed to remove task from schedd' % (colors.RED, colors.NORMAL))
                self.logger.debug('gsissh stdout: %s\ngsissh stderr: %s\ngsissh exitcode: %s' % (stdout,stderr,exitcode))

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option('--schedd',
                               dest = 'scheddonly',
                               action = 'store_true',
                               default = False,
                               help = 'Only clean schedd for the given workflow')

        self.parser.add_option('--cache',
                               dest = 'cacheonly',
                               action = 'store_true',
                               default = False,
                               help = 'Only clearn cache for the given workflow')
