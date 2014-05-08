from os import path
from CRABClient.Commands.SubCommand import SubCommand
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
from CRABClient.client_utilities import colors
from CRABClient.client_utilities import server_info
from CRABClient.client_exceptions import ConfigurationException
class uploadlog(SubCommand):
    """
    Upload the user log to the crab user file cache
    """
    name = 'uploadlog'
    shortnames = ['uplog']

    def __call__(self):

        self.logger.debug("uploadlog started")

        #veryfing the log file exist
        if path.isfile(self.logfile):
            self.logger.debug("crab.log exists")
        else:
            self.logger.info("%sError:%s Could not locate log file" % (colors.RED, colors.NORMAL))
            raise ConfigurationException

        #getting the cache url
        baseurl=self.getUrl(self.instance, resource='info')
        cacheurl=server_info('backendurls', self.serverurl, self.proxyfilename, baseurl)
        cacheurl=cacheurl['cacheSSL']
        cacheurldict={'endpoint' : cacheurl}

        ufc=UserFileCache(cacheurldict)
        logfilename=str(self.cachedinfo['RequestName'])+".log"

        self.logger.debug("cacheURL: %s\nLog file name: %s" % (cacheurl, logfilename))
        self.logger.info("Uploading log file")

        ufc.uploadLog(str(self.logfile), logfilename)

        self.logger.info("%sSuccess:%s Finish uploading log file" % (colors.GREEN, colors.NORMAL))

        logfileurl = cacheurl + '/logfile?name='+str(logfilename)
        self.logger.info("Log file url: %s" %logfileurl)
