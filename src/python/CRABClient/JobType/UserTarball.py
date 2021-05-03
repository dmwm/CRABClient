#! /usr/bin/env python

"""
    UserTarball class, a subclass of TarFile
"""

from  __future__ import division   # make division work like in python3

import json
import os
import glob
import math
import time
import socket
import tarfile
import tempfile
import hashlib
import uuid

import CRABClient.Emulator
from CRABClient.ClientMapping import configParametersInfo
from CRABClient.JobType.ScramEnvironment import ScramEnvironment
from CRABClient.ClientUtilities import colors, BOOTSTRAP_CFGFILE, BOOTSTRAP_CFGFILE_PKL
from CRABClient.ClientExceptions import EnvironmentException, InputFileNotFoundException, CachefileNotFoundException, SandboxTooBigException

from ServerUtilities import NEW_USER_SANDBOX_EXCLUSIONS, BOOTSTRAP_CFGFILE_DUMP
from ServerUtilities import FILE_SIZE_LIMIT
from ServerUtilities import uploadToS3

def testS3upload(s3tester, archiveName, logger):
    hasher = hashlib.sha256()
    with open(archiveName) as f:
        BUF_SIZE = 1024 * 1024  # lets read stuff in 1MByte chunks!
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
        hasher.update(data)
    s3hash = hasher.hexdigest()
    cachename = "%s.tgz" % s3hash
    try:
        t1 = time.time()
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
        msecs = int((t1 - int(t1)) * 1000)
        timestamp += '.%03d' % msecs

        uploadToS3(crabserver=s3tester, objecttype='sandbox', filepath=archiveName,
                   tarballname=cachename, logger=logger)
        status = 'OK'
    except Exception as e:
        logger.exception(str(e))
        status = 'FAIL'
        reason = str(e)
    t2 = time.time()
    s3report = {'status':status}
    if status == 'FAIL':
        s3report['reason'] = reason
    thisSite = socket.gethostname()
    tarballKB = os.stat(archiveName).st_size // 1024
    s3report['timestamp'] = timestamp
    s3report['origin'] = thisSite
    s3report['KBytes'] = tarballKB
    s3report['seconds'] = int(t2-t1)
    return s3report

class UserTarball(object):
    """
        _UserTarball_

            A subclass of TarFile for the user code tarballs. By default
            creates a new tarball with the user libraries from lib, module,
            and the data/ and interface/ sections of the src/ area.

            Also adds user specified files in the right place.
    """

    def __init__(self, name=None, mode='w:bz2', config=None, logger=None, crabserver=None, s3tester=None):
        self.config = config
        self.logger = logger
        self.scram = ScramEnvironment(logger=self.logger)
        self.logger.debug("Making tarball in %s" % name)
        self.tarfile = tarfile.open(name=name , mode=mode, dereference=True)
        self.checksum = None
        self.content = None
        self.crabserver = crabserver
        self.s3tester = s3tester

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

    def printSortedContent(self):
        """
	To be used for diagnostic printouts
        returns a string containing tarball content as a list of files sorted by size
        already formatted for use in a print statement
        """
        sortedContent = sorted(self.content, reverse=True)
        biggestFileSize = sortedContent[0][0]
        ndigits = int(math.ceil(math.log(biggestFileSize+1, 10)))
        contentList  = "\nsandbox content sorted by size[Bytes]:"
        for (size, name) in sortedContent:
            contentList += ("\n%" + str(ndigits) + "s\t%s") % (size, name)
        return contentList


    def upload(self, filecacheurl=None):
        """
        Upload the tarball to the File Cache
        """

        self.close()
        archiveName = self.tarfile.name
        archiveSizeBytes = os.path.getsize(archiveName)

	# in python3 and python2 with __future__ division, double / means integer division
        archiveSizeKB = archiveSizeBytes//1024
        if archiveSizeKB <= 512 :
            archiveSize = "%d KB" % archiveSizeKB
        elif archiveSizeKB < 1024*10 :
            # in python3 and python2 with __future__ division, single / means floating point division
            archiveSize = "%3f.1 MB" % (archiveSizeKB/1024)
        else:
            archiveSize = "%d MB" % (archiveSizeKB//1024)
        if archiveSizeBytes > FILE_SIZE_LIMIT :
            msg=("%sError%s: input tarball size %s exceeds maximum allowed limit of %d MB" % (colors.RED, colors.NORMAL, archiveSize, FILE_SIZE_LIMIT//1024//1024))
            msg += self.printSortedContent()
            raise SandboxTooBigException(msg)

        msg=("Uploading archive %s (%s) to the CRAB cache. Using URI %s" % (archiveName, archiveSize, filecacheurl))
        self.logger.debug(msg)

        if 'S3' in filecacheurl.upper() :
            # use S3
            # generate a 32char hash like UserFileCache used to do
            import hashlib
            hasher = hashlib.sha256()
            with open(archiveName) as f:
                BUF_SIZE = 1024*1024  # lets read stuff in 1MByte chunks!
                while True:
                    data = f.read(BUF_SIZE)
                    if not data:
                        break
                    hasher.update(data)
            hashkey = hasher.hexdigest()
            # the ".tar.gz" suffix here is forced by other places in the client which add it when
            # storing tarball name in task table. Not very elegant to need to hardcode in several places.
            cachename = "%s.tar.gz" % hashkey
            # current code requires a taskname to extract username. Any dummy one will do
            # next version of RESTCache will get username from cmsweb FE headers
            uploadToS3(crabserver=self.crabserver, objecttype='sandbox', filepath=archiveName,
                       tarballname=cachename, logger=self.logger)
        else:
            # old way using UFC
            ufc = CRABClient.Emulator.getEmulator('ufc')({'endpoint' : filecacheurl, "pycurl": True})
            t1 = time.time()
            result = ufc.upload(archiveName, excludeList = NEW_USER_SANDBOX_EXCLUSIONS)
            ufcSeconds = int(time.time()-t1)
            if 'hashkey' not in result:
                self.logger.error("Failed to upload archive: %s" % str(result))
                raise CachefileNotFoundException
            hashkey = str(result['hashkey'])
            # upload a copy to S3 dev as well, just to stress it a bit, this never raises
            s3report = testS3upload(self.s3tester, archiveName, self.logger)
            # upload S3 test report to crabcache
            # report also how log it tool uploading to UFC (which surely worked if we are here)
            s3report['ufcseconds'] = ufcSeconds
            reportFile = '/tmp/crabs3report.' + uuid.uuid4().hex
            with open(reportFile,'w') as fp:
                json.dump(s3report, fp)
            reportName = s3report['timestamp'] + ':s3report.json'
            try:
                ufc.uploadLog(reportFile, reportName)
            except Exception as e:
                self.logger.exception(str(e))
            os.remove(reportFile)
        return hashkey


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
