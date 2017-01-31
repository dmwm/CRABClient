#! /usr/bin/env python

"""
    UserTarball class, a subclass of TarFile
"""

import os
import glob
import tarfile
import tempfile

import CRABClient.Emulator
from CRABClient.ClientMapping import configParametersInfo
from CRABClient.JobType.ScramEnvironment import ScramEnvironment
from CRABClient.ClientUtilities import colors, BOOTSTRAP_CFGFILE, BOOTSTRAP_CFGFILE_PKL
from CRABClient.ClientExceptions import EnvironmentException, InputFileNotFoundException, CachefileNotFoundException

from ServerUtilities import NEW_USER_SANDBOX_EXCLUSIONS, BOOTSTRAP_CFGFILE_DUMP


class UserTarball(object):
    """
        _UserTarball_

            A subclass of TarFile for the user code tarballs. By default
            creates a new tarball with the user libraries from lib, module,
            and the data/ and interface/ sections of the src/ area.

            Also adds user specified files in the right place.
    """

    def __init__(self, name=None, mode='w:gz', config=None, logger=None):
        self.config = config
        self.logger = logger
        self.scram = ScramEnvironment(logger=self.logger)
        self.logger.debug("Making tarball in %s" % name)
        self.tarfile = tarfile.open(name=name , mode=mode, dereference=True)
        self.checksum = None

    def addFiles(self, userFiles=None, cfgOutputName=None):
        """
        Add the necessary files to the tarball
        """
        directories = ['lib', 'biglib', 'module']
        if getattr(self.config.JobType, 'sendPythonFolder', configParametersInfo['JobType.sendPythonFolder']['default']):
            directories.append('python')
            directories.append('cfipython')
        if getattr(self.config.JobType, 'sendExternalFolder', configParametersInfo['JobType.sendExternalFolder']['default']):
            externalDirPath = os.path.join(self.scram.getCmsswBase(), 'external')
            if os.path.exists(externalDirPath) and os.listdir(externalDirPath) != []:
                directories.append('external')
            else:
                self.logger.info("The config.JobType.sendExternalFolder parameter is set to True but the external directory "\
                                  "doesn't exist or is empty, not adding to tarball. Path: %s" % externalDirPath)

        # Note that dataDirs are only looked-for and added under the src/ folder.
        # /data/ subdirs contain data files needed by the code
        # /interface/ subdirs contain C++ header files needed e.g. by ROOT6
        dataDirs    = ['data','interface']
        userFiles = userFiles or []

        # Tar up whole directories
        for directory in directories:
            fullPath = os.path.join(self.scram.getCmsswBase(), directory)
            self.logger.debug("Checking directory %s" % fullPath)
            if os.path.exists(fullPath):
                self.logger.debug("Adding directory %s to tarball" % fullPath)
                self.checkdirectory(fullPath)
                self.tarfile.add(fullPath, directory, recursive=True)

        # Search for and tar up "data" directories in src/
        srcPath = os.path.join(self.scram.getCmsswBase(), 'src')
        for root, _, _ in os.walk(srcPath):
            if os.path.basename(root) in dataDirs:
                directory = root.replace(srcPath,'src')
                self.logger.debug("Adding data directory %s to tarball" % root)
                self.checkdirectory(root)
                self.tarfile.add(root, directory, recursive=True)

        # Tar up extra files the user needs
        for globName in userFiles:
            fileNames = glob.glob(globName)
            if not fileNames:
                raise InputFileNotFoundException("The input file '%s' taken from parameter config.JobType.inputFiles cannot be found." % globName)
            for filename in fileNames:
                self.logger.debug("Adding file %s to tarball" % filename)
                self.checkdirectory(filename)
                self.tarfile.add(filename, os.path.basename(filename), recursive=True)


        scriptExe = getattr(self.config.JobType, 'scriptExe', None)
        if scriptExe:
            self.tarfile.add(scriptExe, arcname=os.path.basename(scriptExe))

        # Adding the pset files to the tarfile
        if cfgOutputName:
            basedir = os.path.dirname(cfgOutputName)
            self.tarfile.add(cfgOutputName, arcname=BOOTSTRAP_CFGFILE)
            self.tarfile.add(os.path.join(basedir, BOOTSTRAP_CFGFILE_PKL), arcname=BOOTSTRAP_CFGFILE_PKL)
            self.tarfile.add(os.path.join(basedir, BOOTSTRAP_CFGFILE_DUMP), arcname=BOOTSTRAP_CFGFILE_DUMP)

    def addMonFiles(self):
        """
        Add monitoring files the debug tarball.
        """
        configtmp = tempfile.NamedTemporaryFile(delete=True)
        configtmp.write(str(self.config))
        configtmp.flush()
        psetfilename = getattr(self.config.JobType, 'psetName', None)
        if not psetfilename == None:
            self.tarfile.add(psetfilename,'/debug/originalPSet.py')
        else:
            self.logger.debug('Failed to add pset to debug_files.tar.gz')

        self.tarfile.add(configtmp.name, '/debug/crabConfig.py')

        scriptExe = getattr(self.config.JobType, 'scriptExe', None)
        if scriptExe:
            self.tarfile.add(scriptExe, arcname=os.path.basename(scriptExe))

        configtmp.close()

    def writeContent(self):
        """Save the content of the tarball"""
        self.content = [(int(x.size), x.name) for x in self.tarfile.getmembers()]


    def close(self):
        """
        Calculate the checkum and close
        """
        self.writeContent()
        return self.tarfile.close()


    def upload(self, filecacheurl=None):
        """
        Upload the tarball to the File Cache
        """

        self.close()
        archiveName = self.tarfile.name
	archiveSizeKB = os.path.getsize(archiveName)/1024
	if archiveSizeKB <= 512 :
	  archiveSize = "%d KB" % archiveSizeKB
	elif archiveSizeKB < 1024*10 :
	  archiveSize = "%3f.1 MB" % (archiveSizeKB/1024.)
	else:
	  archiveSize = "%d MB" % (archiveSizeKB/1024)
	msg=("Uploading archive %s (%s) to the CRAB cache. Using URI %s" % (archiveName, archiveSize, filecacheurl))
	self.logger.debug(msg)

	ufc = CRABClient.Emulator.getEmulator('ufc')({'endpoint' : filecacheurl, "pycurl": True})
        result = ufc.upload(archiveName, excludeList = NEW_USER_SANDBOX_EXCLUSIONS)
        if 'hashkey' not in result:
            self.logger.error("Failed to upload source files: %s" % str(result))
            raise CachefileNotFoundException
        return str(result['hashkey'])


    def checkdirectory(self, dir_):
        #checking for infinite symbolic link loop
        try:
            for root , _ , files in os.walk(dir_, followlinks = True):
                for file_ in files:
                    os.stat(os.path.join(root, file_ ))
        except OSError as msg:
            err = '%sError%s: Infinite directory loop found in: %s \nStderr: %s' % \
                    (colors.RED, colors.NORMAL, dir_ , msg)
            raise EnvironmentException(err)


    def __getattr__(self, *args):
        """
        Pass any unknown functions or attribute requests on to the TarFile object
        """
        self.logger.debug("Passing getattr %s on to TarFile" % args)
        return self.tarfile.__getattribute__(*args)


    def __enter__(self):
        """
        Allow use as context manager
        """
        return self


    def __exit__(self, excType, excValue, excTrace):
        """
        Allow use as context manager
        """
        self.tarfile.close()
        if excType:
            return False
