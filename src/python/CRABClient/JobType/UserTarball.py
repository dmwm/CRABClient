#! /usr/bin/env python

"""
    UserTarball class, a subclass of TarFile
"""

from  __future__ import division   # make division work like in python3

import os
import glob
import math
import tarfile
import tempfile
import shutil
import hashlib

from ServerUtilities import NEW_USER_SANDBOX_EXCLUSIONS, BOOTSTRAP_CFGFILE_DUMP
from ServerUtilities import FILE_SIZE_LIMIT
from ServerUtilities import uploadToS3

from CRABClient.ClientMapping import configParametersInfo
from CRABClient.JobType.ScramEnvironment import ScramEnvironment
from CRABClient.ClientUtilities import colors, BOOTSTRAP_CFGFILE, BOOTSTRAP_CFGFILE_PKL
from CRABClient.ClientExceptions import EnvironmentException, InputFileNotFoundException, SandboxTooBigException
from CRABClient.ClientUtilities import execute_command


def calculateChecksum(tarfile_, exclude=None):
    """
    Imported here from WMCore/Services/UserFileCache.py
    Originally written by Marco Mascheroni: refs
    https://github.com/mmascher/WMCore/commit/01855223030c4936234be62df6a5a0b2a911144e
    https://github.com/dmwm/WMCore/commit/5e910461eb82e7bfcba473def6511e3f94259672
    https://github.com/dmwm/CRABServer/issues/4948

    Calculate the checksum of the tar file in input.
    The tarfile_ input parameter could be a string or a file object (anything compatible
    with the fileobj parameter of tarfile.open).
    The exclude parameter could be a list of strings, or a callable that takes as input
    the output of  the list of tarfile.getmembers() and return a list of strings.
    The exclude param is interpreted as a list of files that will not be taken into consideration
    when calculating the checksum.
    The output is the checksum of the tar input file.
    The checksum is calculated taking into consideration the names of the objects
    in the tarfile (files, directories etc) and the content of each file.
    Each file is exctracted, read, and then deleted right after the input is passed
    to the hasher object. The file is read in chuncks of 4096 bytes to avoid memory
    issues.
    """
    if not exclude:  # [] is a dangerous value for a param
        exclude = []

    hasher = hashlib.sha256()

    ## "massage" out the input parameters
    if isinstance(tarfile_, (str, bytes)):
        tar = tarfile.open(tarfile_, mode='r')
    else:
        tar = tarfile.open(fileobj=tarfile_, mode='r')

    if exclude and hasattr(exclude, '__call__'):
        excludeList = exclude(tar.getmembers())
    else:
        excludeList = exclude

    tmpDir = tempfile.mkdtemp()
    try:
        for tarmember in tar:
            if tarmember.name in excludeList:
                continue
            hasher.update(tarmember.name.encode('utf-8'))
            if tarmember.isfile() and tarmember.name.split('.')[-1] != 'pkl':
                tar.extractall(path=tmpDir, members=[tarmember])
                fn = os.path.join(tmpDir, tarmember.name)
                with open(fn, 'rb') as fd:
                    while True:
                        buf = fd.read(4096)
                        if not buf:
                            break
                        hasher.update(buf)
                os.remove(fn)
    finally:
        # never leave tmddir around
        shutil.rmtree(tmpDir)
    checksum = hasher.hexdigest()

    return checksum


def excludeFromTar(tarinfo):
    """
    some files or directories should never go in the sandbox
       .git subdirectory https://github.com/dmwm/CRABClient/issues/5202
       scram pre-built objects used since CMSSW_13 https://github.com/dmwm/CRABClient/issues/5300
    """
    if '.git' in tarinfo.name:
        return None
    if 'objs-base' in tarinfo.name or 'objs-full' in tarinfo.name:
        return None
    return tarinfo


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
        self.tarfile = tarfile.open(name=name, mode=mode, dereference=True)
        self.checksum = None
        self.content = None
        self.crabserver = crabserver
        self.s3tester = s3tester

    def addFiles(self, userFiles=None, cfgOutputName=None):
        """
        Add the necessary files to the tarball
        """

        # Tar up whole directories in $CMSSW_BASE/
        directories = ['bin', 'python', 'cfipython', 'lib', 'biglib', 'module', 'config/SCRAM/hooks']
        if getattr(self.config.JobType, 'sendExternalFolder', configParametersInfo['JobType.sendExternalFolder']['default']):
            externalDirPath = os.path.join(self.scram.getCmsswBase(), 'external')
            if os.path.exists(externalDirPath) and os.listdir(externalDirPath) != []:
                directories.append('external')
            else:
                self.logger.info("The config.JobType.sendExternalFolder parameter is set to True but the external directory "\
                                  "doesn't exist or is empty, not adding to tarball. Path: %s" % externalDirPath)

        for directory in directories:
            fullPath = os.path.join(self.scram.getCmsswBase(), directory)
            self.logger.debug("Checking directory %s" % fullPath)
            if os.path.exists(fullPath):
                self.logger.debug("Adding directory %s to tarball" % fullPath)
                self.checkdirectory(fullPath)
                archiveDir = os.path.join(self.scram.getCmsswVersion(), directory)
                self.tarfile.add(fullPath, archiveDir, recursive=True, filter=excludeFromTar)

        # Recursively search for and add to tar some directories in $CMSSW_BASE/src/
        # Note that recursiveDirs are **only** looked-for under the $CMSSW_BASE/src/ folder!
        # /data/      subdirs contain data files needed by the code
        # /interface/ subdirs contain C++ header files needed e.g. by ROOT6
        #             Shahzad suggested adding to the sandbox only src/*/*/interface, but
        #             we currently add all the src/**/interface, see:
        #             https://cms-talk.web.cern.ch/t/missing-symbolic-links-in-cmssw-base-python/20403/35
        #             Since we have not found a problem yet, Dario will not change it.
        # /python/    subdirs contain user python code, see https://github.com/dmwm/CRABClient/issues/5187
        #             Shahzad asked that we add to the tar only src/*/*/python, but we tar all the src/**/python
        #             this is because Dario preferred to re-use existing code for /data/ and /interface/
        recursiveDirs = ['data', 'interface', 'python']
        srcPath = os.path.join(self.scram.getCmsswBase(), 'src')
        for root, _, _ in os.walk(srcPath):
            if os.path.basename(root) in recursiveDirs:
                directory = root.replace(srcPath, 'src')
                self.logger.debug("Adding directory %s to tarball" % root)
                self.checkdirectory(root)
                archiveDir = os.path.join(self.scram.getCmsswVersion(), directory)
                self.tarfile.add(root, archiveDir, recursive=True, filter=excludeFromTar)

        # Tar up extra files the user needs
        userFiles = userFiles or []
        for globName in userFiles:
            fileNames = glob.glob(globName)
            if not fileNames:
                raise InputFileNotFoundException("The input file '%s' taken from parameter config.JobType.inputFiles cannot be found." % globName)
            for filename in fileNames:
                self.logger.debug("Adding file %s to tarball" % filename)
                self.checkdirectory(filename)
                self.tarfile.add(filename, os.path.basename(filename), recursive=True, filter=excludeFromTar)

        scriptExe = getattr(self.config.JobType, 'scriptExe', None)
        if scriptExe:
            self.tarfile.add(scriptExe, arcname=os.path.basename(scriptExe))

        # Adding the pset files to the tarfile
        if cfgOutputName:
            basedir = os.path.dirname(cfgOutputName)
            self.tarfile.add(cfgOutputName, arcname=BOOTSTRAP_CFGFILE)
            self.tarfile.add(os.path.join(basedir, BOOTSTRAP_CFGFILE_PKL), arcname=BOOTSTRAP_CFGFILE_PKL)
            self.tarfile.add(os.path.join(basedir, BOOTSTRAP_CFGFILE_DUMP), arcname=BOOTSTRAP_CFGFILE_DUMP)

    def addVenvDirectory(self, tarFile='sandbox.tgz'):
        # adds CMSSW_BASE/venv directory to the (closed, compressed) sandbox
        # venv directory is special because symbolic links have to be kept as such (no dereference)
        # this requires tarfile to be closed and opened with a different dereference flag,
        # but appending can only work on non-compressed tar files !

        venv = os.path.join(self.scram.getCmsswBase(), 'venv')
        # have to do some file renaming because bzip2 needs its onw extention
        # this will be cleaner once we rename upstream sandbox.tgz to sandbox.tbz !)

        # prepare names:
        tarFileName = os.path.splitext(tarFile)[0]  # remove .tgz
        uncompressed = tarFileName + '.tar'  # sandbox.tar
        bzipped= uncompressed + '.bz2'  # sandbox.tar.bz2
        # now do the work
        cmd = 'mv %s %s' % (tarFile, bzipped)  # rename to what bunzip2 likes
        cmd += '; bunzip2 ' + bzipped  # uncompress, creates sandbox.tar
        execute_command(cmd, logger=self.logger)
        # use python tarfile to append, so can rename file inside the archive
        tar = tarfile.open(name=uncompressed, mode='a', dereference=False)
        archiveDir = os.path.join(self.scram.getCmsswVersion(), 'venv')
        tar.add(name=venv, arcname=archiveDir)
        tar.close()
        cmd = 'bzip2 ' + uncompressed  # compress, creates sandbox.tar.bz2
        cmd += "; mv %s %s" % (bzipped, tarFile)  # rename to sandbox.tgz
        execute_command(cmd, logger=self.logger)

    def addMonFiles(self):
        """
        Add monitoring files the debug tarball.
        """
        configtmp = tempfile.NamedTemporaryFile(mode='w', delete=True)
        configtmp.write(str(self.config))
        configtmp.flush()
        psetfilename = getattr(self.config.JobType, 'psetName', None)
        if psetfilename:
            self.tarfile.add(psetfilename, '/debug/originalPSet.py')
        else:
            self.logger.debug('Failed to add pset to debug_files.tar.gz')

        self.tarfile.add(configtmp.name, '/debug/crabConfig.py')

        scriptExe = getattr(self.config.JobType, 'scriptExe', None)
        if scriptExe:
            self.tarfile.add(scriptExe, arcname=os.path.join('/debug', os.path.basename(scriptExe)))

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

    def printSortedContent(self, maxLines=None):
        """
	To be used for diagnostic printouts
        returns a string containing tarball content as a list of files sorted by size
        already formatted for use in a print statement
        if Max is specified, only the largest max files are listed
        """
        sortedContent = sorted(self.content, reverse=True)
        biggestFileSize = sortedContent[0][0]
        ndigits = int(math.ceil(math.log(biggestFileSize+1, 10)))
        contentList = "\nsandbox content sorted by size[Bytes]:"
        n = 0
        for (size, name) in sortedContent:
            contentList += ("\n%" + str(ndigits) + "s\t%s") % (size, name)
            n += 1
            if maxLines and n == maxLines:
                break
        return contentList

    def upload(self, filecacheurl=None):
        """
        Upload the tarball to the File Cache
        """

        self.close()
        archiveName = self.tarfile.name

        # #CMSSW_BASE/venv is special, needs to be done here
        # after all sing and dance is over and the tar is closed !
        if getattr(self.config.JobType, 'sendVenvFolder', configParametersInfo['JobType.sendVenvFolder']['default']):
            self.addVenvDirectory(tarFile=archiveName)

        archiveSizeBytes = os.path.getsize(archiveName)

	# in python3 and python2 with __future__ division, double / means integer division
        archiveSizeKB = archiveSizeBytes//1024
        if archiveSizeKB <= 512:
            archiveSize = "%d KB" % archiveSizeKB
        elif archiveSizeKB < 1024*10:
            # in python3 and python2 with __future__ division, single / means floating point division
            archiveSize = "%3f.1 MB" % (archiveSizeKB/1024)
        else:
            archiveSize = "%d MB" % (archiveSizeKB//1024)
        if archiveSizeBytes > FILE_SIZE_LIMIT:
            msg = ("%sError%s: input tarball size %s exceeds maximum allowed limit of %d MB" %
                   (colors.RED, colors.NORMAL, archiveSize, FILE_SIZE_LIMIT//1024//1024))
            msg += "\nlargest 5 files are:"
            msg += self.printSortedContent(maxLines=5)
            msg += "\nsee crab.log file for full list of tarball content"
            fullList = self.printSortedContent()
            self.logger.debug(fullList)
            raise SandboxTooBigException(msg)

        msg = ("Uploading archive %s (%s) to the CRAB cache. Using URI %s" %
               (archiveName, archiveSize, filecacheurl))
        self.logger.debug(msg)

        # generate a 32char hash like old UserFileCache used to do
        hashkey = calculateChecksum(archiveName, exclude=NEW_USER_SANDBOX_EXCLUSIONS)
        # the ".tar.gz" suffix here is forced by other places in the client which add it when
        # storing tarball name in task table. Not very elegant to need to hardcode in several places.
        cachename = "%s.tar.gz" % hashkey
        # current code requires a taskname to extract username. Any dummy one will do
        # next version of RESTCache will get username from cmsweb FE headers
        uploadToS3(crabserver=self.crabserver, objecttype='sandbox', filepath=archiveName,
                   tarballname=cachename, logger=self.logger)
        return hashkey


    def checkdirectory(self, dir_):
        #checking for infinite symbolic link loop
        try:
            for root, _, files in os.walk(dir_, followlinks=True):
                for file_ in files:
                    os.stat(os.path.join(root, file_))
        except OSError as msg:
            err = '%sError%s: Infinite directory loop found in: %s \nStderr: %s' % \
                    (colors.RED, colors.NORMAL, dir_, msg)
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
