import os

from CRABClient.Commands.SubCommand import SubCommand

from CRABClient.UserUtilities import curlGetFileFromURL, getColumn

from ServerUtilities import downloadFromS3, getProxiedWebDir


class getsandbox(SubCommand):
    """
    given a projdir, downloads locally the user sandbox.
    It will try s3 first, otherwise it will fall back to the schedd WEBDIR.
    """

    name = "getsandbox"

    def __call__(self):

        # init. debug. print useful info
        self.logger.debug("requestarea: %s", self.requestarea)
        self.logger.debug("cachedinfo: %s", self.cachedinfo)

        # get information necessary for next steps
        # Get all of the columns from the database for a certain task
        self.taskname = self.cachedinfo['RequestName']
        self.crabDBInfo, _, _ = self.crabserver.get(api='task', data={'subresource':'search', 'workflow':self.taskname})
        self.logger.debug("Got information from server oracle database: %s", self.crabDBInfo)

        # arguments used by following functions
        self.downloadDir = os.path.join(self.requestarea, "taskconfig")
    
        # download files: user sandbox, debug sandbox
        filelist = []
        # usersandbox = self.downloadUserSandbox()
        usersandbox = self.downloadSandbox(
            remotefile=getColumn(self.crabDBInfo, 'tm_user_sandbox'),
            localfile='sandbox.tar.gz')
        filelist.append(usersandbox)
        # debugfiles = self.downloadDebug()
        debugfiles = self.downloadSandbox(
            remotefile=getColumn(self.crabDBInfo, 'tm_debug_files'),
            localfile='debug_files.tar.gz')
        filelist.append(debugfiles)

        returnDict = {"commandStatus": "FAILED"}
        if filelist:
            returnDict = {"commandStatus": "SUCCESS", "sandbox_paths": filelist }

        return returnDict

    def downloadSandbox(self, remotefile, localfile):
        """
        Copy remotefile from s3 to localfile on local disk.

        If remotefile is not s3, then as a fallback we look for the corresponding
        localfile in the schedd webdir.
        """
        username = getColumn(self.crabDBInfo, 'tm_username')
        sandboxFilename = remotefile

        self.logger.debug("will download sandbox from s3: %s",sandboxFilename)

        if not os.path.isdir(self.downloadDir):
            os.mkdir(self.downloadDir)
        localSandboxPath = os.path.join(self.downloadDir, localfile)

        try:
            downloadFromS3(crabserver=self.crabserver,
                        filepath=localSandboxPath,
                        objecttype='sandbox', logger=self.logger,
                        tarballname=sandboxFilename,
                        username=username
                        )
        except Exception as e:
            self.logger.info("Sandbox download failed with %s", e)
            self.logger.info("We will look for the sandbox on the webdir of the schedd")

            webdir = getProxiedWebDir(crabserver=self.crabserver, task=self.taskname,
                                    logFunction=self.logger.debug)
            if not webdir:
                webdir = getColumn(self.crabDBInfo, 'tm_user_webdir')
            self.logger.debug("Downloading %s from %s", localfile, webdir)
            httpCode = curlGetFileFromURL(webdir + '/' + localfile,
                                        localSandboxPath, self.proxyfilename,
                                        logger=self.logger)
            if httpCode != 200:
                self.logger.error("Failed to download %s from %s", localfile, webdir)
                raise Exception("We could not locate the sandbox in the webdir neither.")
                # we should use
                # raise Exception("We could not locate the sandbox in the webdir neither.") from e
                # but that is not py2 compatible...

        return localSandboxPath

