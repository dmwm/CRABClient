#! /usr/bin/env python

"""
    UserTarball class, a subclass of TarFile
"""

import commands
import glob
import json
import os
import tarfile
import tempfile
import hashlib
import sys

from CRABClient.JobType.ScramEnvironment import ScramEnvironment
from CRABClient.client_exceptions import InputFileNotFoundException, CachefileNotFoundException
import CRABClient.PandaInterface as PandaInterface
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
from WMCore.Configuration import loadConfigurationFile, Configuration

class UserTarball(object):
    """
        _UserTarball_

            A subclass of TarFile for the user code tarballs. By default
            creates a new tarball with the user libraries from lib, module,
            and the data/ sections of the src/ area.

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
        directories = ['lib', 'module']
        dataDirs    = ['data']
        userFiles = userFiles or []

        # Tar up whole directories
        for directory in  directories:
            fullPath = os.path.join(self.scram.getCmsswBase(), directory)
            self.logger.debug(" checking directory %s" % fullPath)
            if os.path.exists(fullPath):
                self.logger.debug(" adding directory %s to tarball" % fullPath)
                self.tarfile.add(fullPath, directory, recursive=True)

        # Search for and tar up "data" directories in src/
        srcPath = os.path.join(self.scram.getCmsswBase(), 'src')
        for root, _dummy, _dummy in os.walk(srcPath):
            if os.path.basename(root) in dataDirs:
                directory = root.replace(srcPath,'src')
                self.logger.debug(" adding data directory %s to tarball" % root)
                self.tarfile.add(root, directory, recursive=True)

        # Tar up extra files the user needs
        for globName in userFiles:
            fileNames = glob.glob(globName)
            if not fileNames:
                raise InputFileNotFoundException('The input file "%s" taken from parameter config.JobType.inputFiles cannot be found' % globName)
            for filename in fileNames:
                self.logger.debug(" adding file %s to tarball" % filename)
                self.tarfile.add(filename, os.path.basename(filename), recursive=True)

        # Adding the pset file to the tarfile
        if cfgOutputName:
            self.tarfile.add(cfgOutputName, arcname='PSet.py')
        currentPath = os.getcwd()

#        psetfile = getattr(self.config.JobType, 'psetName', None)
#        self.tarfile.add(os.path.join(currentPath, psetfile), arcname='PSet.py')

    def close(self):
        """
        Calculate the checkum and clos
        """

        self.calculateChecksum()
        return self.tarfile.close()

    def upload(self):
        """
        Upload the tarball to the Panda Cache
        """
        self.close()
        archiveName = self.tarfile.name
        serverUrl = ""
        self.logger.debug(" uploading archive to cache %s " % archiveName)
        status,out = PandaInterface.putFile(archiveName, verbose=False, useCacheSrv=True, reuseSandbox=True)

        if out.startswith('NewFileName:'):
            # found the same input sandbox to reuse
            self.logger.debug("out: %s" % out)
            self.logger.debug("status: %s" % status)
            self.logger.debug("found the same input sandbox to reuse")
            archiveName = out.split(':')[-1]
            serverUrl = "https://%s:%s" % (out.split(':')[-2], '25443')
            self.logger.debug("archiveName: %s" %archiveName)
        elif out.startswith('True'):
            archiveName = out.split(':')[-1]
            serverUrl = "%s:%s:%s" % (out.split(':')[-4], out.split(':')[-3], out.split(':')[-2])
        else:
            self.logger.error( str(out) )
            self.logger.error("failed to upload source files with %s" % status)
            raise CachefileNotFoundException

        #XXX: I dont like the /userfilecache/data/file hardcoded
        #ufc = UserFileCache({'endpoint': "https://" + self.config.General.ufccacheUrl + "/crabcache", \
        #                                  "proxyfilename" : self.config.JobType.proxyfilename, "capath" : self.config.JobType.capath, "newrest" : True})

        #return ufc.upload(self.tarfile.name)
        return serverUrl, archiveName

    def calculateChecksum(self):
        """
        Calculate a checksum that doesn't depend on the tgz
        creation data
        """

        lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in self.tarfile.getmembers()]
        hasher = hashlib.sha256(str(lsl))
        self.logger.debug('tgz contents: %s' % lsl)
        self.checksum = hasher.hexdigest()
        self.logger.debug('Checksum: %s' % self.checksum)

        #Old way reads in the file again. May use for for non-tar files if needed.
        #sha256sum = hashlib.sha256()
        #with open(self.tarfile.name, 'rb') as f:
            #while True:
                #chunkdata = f.read(8192)
                #if not chunkdata:
                    #break
                #sha256sum.update(chunkdata)
        #sha256sum.hexdigest()


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
