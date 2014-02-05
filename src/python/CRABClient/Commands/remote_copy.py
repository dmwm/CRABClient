from __future__ import division
import os
import subprocess
import multiprocessing, Queue
import time
import re
from math import ceil
from multiprocessing import Manager

from WMCore.FwkJobReport.FileInfo import readAdler32, readCksum

from CRABClient.Commands import CommandResult
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.client_utilities import colors


class remote_copy(SubCommand):

    ## currently doesn't need to be showed to the outside
    visible = False

    name  = __name__.split('.').pop()
    usage = "usage: %prog " + name + " [options] [args]"


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option( "-d", "--destination",
                                dest = "destination",
                                default = None )

        self.parser.add_option( "-i", "--input",
                                dest = "inputdict",
                                default = None )

        self.parser.add_option("-l", "--parallel",
                                dest = "nparallel")

    def __call__(self):
        """
        Copying locally files staged remotely.
         *Using a subprocess to encapsulate the copy command.
         *Using a timeout to avoid wiating too long
           - srm timeout based on file size
           - first transfer assumes a relatively slow bandiwth `downspeed`
           - next transfers depends on previous speed
           - srm timeout cannot be less then `minsrmtimeout`
           - if size is unknown default srm timeout is `srmtimeout`
        """

        globalExitcode = -1

        dicttocopy = self.options.inputdict

        #taking number of parallel download to create from user, default is 10

        if self.options.nparallel==None:
            nsubprocess=10
        else:
            nsubprocess=int(self.options.nparallel)

        if nsubprocess <=0 or nsubprocess >20:
            self.logger.info("Inappropriate number of parallel download, must between 0 to 20 ")
            return -1

        lcgCmd = 'lcg-cp --connect-timeout 20 --sendreceive-timeout 240 --verbose -b -D srmv2'
        lcgtimeout = 20 + 240 + 60 #giving 1 extra minute: 5min20"
        srmtimeout = 900 #default transfer timeout in case the file size is unknown: 15min
        minsrmtimeout = 60 #timeout cannot be less then 1min
        downspeed = float(250*1024) #default speed assumes a download of 250KB/s
        mindownspeed = 20*1024.

        manager=Manager()
        successfiles = manager.dict()
        failedfiles = manager.dict()


        self.logger.debug("Starting ChildProcess with %s ChildProcess" % nsubprocess)
        inputq, processarray = self.startchildproc(self.processWorker,nsubprocess, successfiles, failedfiles)

        for myfile in dicttocopy:
            if downspeed < mindownspeed:
                downspeed = mindownspeed

            fileid = myfile['pfn'].split('/')[-1]

            dirpath = os.path.join(self.options.destination, myfile['suffix'] if 'suffix' in myfile else '')
            url_input = bool(re.match("^[a-z]+://", dirpath))
            if not url_input and not os.path.isdir(dirpath):
                os.makedirs(dirpath)
            localFilename = os.path.join(dirpath,  str(fileid))

            ##### Handling the "already existing file" use case
            if not url_input and os.path.isfile(localFilename):
                size = os.path.getsize(localFilename)
                #delete the file if its size is zero or its size is not the expected size
                if size==0 or ('size' in myfile and myfile['size']!=size):
                    try:
                        self.logger.info("Removing %s as it is not complete: current size %s, expected size %s" % (fileid, size, \
                                                                                myfile['size'] if 'size' in myfile else 'unknown'))
                        os.remove(localFilename)
                    except Exception, ex:
                        self.logger.info("Cannot remove the file because of: %s" % ex)
            #if the file still exists skip it
            if not url_input and os.path.isfile(localFilename):
                self.logger.info("%sSkipping %s as %s already exists%s" % (colors.GREEN, fileid, localFilename, colors.NORMAL))
                continue

            ##### Creating the command
            maxtime = srmtimeout if not 'size' in myfile or myfile['size']==0 else int(ceil(2*myfile['size']/downspeed)) #timeout based on file size and download speed * 2
            localsrmtimeout = minsrmtimeout if maxtime < minsrmtimeout else maxtime #do not want a too short timeout
            cmd = '%s %s %s %%s' % (lcgCmd, ' --srm-timeout ' + str(localsrmtimeout) + ' ', myfile['pfn'])
            if url_input:
                cmd = cmd % localFilename
            else:
                cmd = cmd % ("file://%s" % localFilename)

            self.logger.info("Placing file '%s' in retrieval queue " % fileid)
            inputq.put((myfile, cmd))


        self.logger.info("Please wait")


        self.stopchildproc(inputq, processarray,nsubprocess)


        #getting output for global exit
        if len(successfiles)==0:
            self.logger.info("No file retrieved")
            globalExitcode= -1
        elif len(failedfiles) != 0:
            self.logger.info(colors.GREEN+"Number of file successfully retrieve: %s" % len(successfiles)+colors.NORMAL)
            self.logger.info(colors.RED+"Number of file failed retrieve: %s" % len(failedfiles)+colors.NORMAL)
            self.logger.debug("List of failed file and reason: %s" % failedfiles)
            globalExitcode= -1
        else:
            self.logger.info(colors.GREEN+"All fails successfully retrieve ")
            globalExitcode=0




        return CommandResult(globalExitcode, '')

    def startchildproc(self, childprocess, nsubprocess, successfiles, failedfiles):
        """
        starting sub process and creating the queue
        """
        inputq  = multiprocessing.Queue()
        subprocessarray=[]

        for i in xrange(nsubprocess):
            p = multiprocessing.Process(target = childprocess, args = (inputq, successfiles, failedfiles))
            subprocessarray.append(p)
            subprocessarray[i].start()


        return inputq,subprocessarray

    def stopchildproc(self,inputq,processarray,nsubprocess):
        """
        simply sending a STOP message to the sub process
        """
        self.logger.debug("stopchildproc() method has been called")
        try:
            for i in range(nsubprocess):
                inputq.put(('-1', 'STOP'))

        #except Exception, ex:
         #   pass
        finally:
            # giving the time to the sub-process to exit
            for process in processarray:
                process.join()
                #time.sleep(1)

    def processWorker(self,input, successfiles, failedfiles):
        """
        _processWorker_

        Runs a subprocessed command.
        """
        # Get this started


        while True:
            workid = None
            try:
                myfile, work = input.get()
                t1 = time.time()
            except (EOFError, IOError):
                crashMessage = "Hit EOF/IO in getting new work\n"
                crashMessage += "Assuming this is a graceful break attempt."
                print crashMessage
                break

            if work == 'STOP':
                break
            else:
                fileid = myfile['pfn'].split('/')[-1]
                dirpath = os.path.join(self.options.destination, myfile['suffix'] if 'suffix' in myfile else '')
                if not os.path.isdir(dirpath):
                    os.makedirs(dirpath)
                url_input = bool(re.match("^[a-z]+://", dirpath))
                localFilename = os.path.join(dirpath,  str(fileid))
                command = work


            pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                                     stderr = subprocess.PIPE, shell = True)
            stdout, stderr = pipe.communicate()

            if pipe.returncode != 0:
                self.logger.info(colors.RED + "Failed retrieving %s" % fileid + colors.NORMAL)
                error=simpleOutputCheck(stderr)
                failedfiles[fileid]=str(error)
                if os.path.isfile(localFilename) and os.path.getsize(localFilename)!=myfile['size']:
                    self.logger.debug("File %s has the wrong size, deleting it" % fileid)
                    try:
                        os.remove(localFilename)
                    except Exception, ex:
                        self.logger.debug("Cannot remove the file because of: %s" % ex)
                time.sleep(60)
                return 0
            else:
                self.logger.info(colors.GREEN + "Success in retrieving %s " %fileid + colors.NORMAL)
            if not url_input and hasattr(myfile, 'checksum'):
                self.logger.debug("Checksum '%s'" %str(myfile['checksum']))
                checksumOK = checksumChecker(localFilename, myfile['checksum'])
            else:
                checksumOK = True # No checksums provided

            if not checksumOK:
                failedfiles[fileid]="Checksum failed"
            else:
                successfiles[fileid]=' Successfully retrieve'
        return 0


def simpleOutputCheck(outlines):
    """
    paree line by line the outlines text lookng for Exceptions
    """
    problems = []
    lines = []
    if outlines.find("\n") != -1:
        lines = outlines.split("\n")
    else:
        lines = [outlines]
    for line in lines:
        line = line.lower()
        if line.find("no entries for host") != -1 or\
           line.find("srm client error") != -1:
            problems.append(line)
        elif line.find("command not found") != -1:
            problems.append(line)
        elif line.find("user has no permission") != -1 or\
             line.find("permission denied") != -1:
            problems.append(line)
        elif line.find("file exists") != -1:
            problems.append(line)
        elif line.find("no such file or directory") != -1 or \
             line.find("error") != -1 or line.find("Failed") != -1 or \
             line.find("cacheexception") != -1 or \
             line.find("does not exist") != -1 or \
             line.find("not found") != -1 or \
             line.find("could not get storage info by path") != -1:
            cacheP = line.split(":")[-1]
            problems.append(cacheP)
        elif line.find("unknown option") != -1 or \
             line.find("unrecognized option") != -1 or \
             line.find("invalid option") != -1:
            problems.append(line)
        elif line.find("timeout") != -1 or \
             line.find("timed out") != -1:
            problems.append(line)

    return set(problems)


def checksumChecker(localFilename, checksums):
    """
    Check given checksums vs. what's on disk
    """
    try:
        adler32 = readAdler32(localFilename)
        if adler32 == checksums['adler32']:
            return True
        else:
            return False
    except:
        cksum = readCksum(localFilename)
        if cksum == checksums['cksum']:
            return True
        else:
            return False

    return False
