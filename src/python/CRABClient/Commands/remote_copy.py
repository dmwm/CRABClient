from __future__ import division
import os
import logging
import subprocess
import threading
import multiprocessing, Queue
import time
from math import ceil

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

        lcgCmd = 'lcg-cp --connect-timeout 20 --sendreceive-timeout 240 --verbose -b -D srmv2'
        lcgtimeout = 20 + 240 + 60 #giving 1 extra minute: 5min20"
        srmtimeout = 900 #default transfer timeout in case the file size is unknown: 15min
        minsrmtimeout = 60 #timeout cannot be less then 1min
        downspeed = float(250*1024) #default speed assumes a download of 250KB/s

        finalresults = {}

        #this can be parallelized starting more processes in startchildproc
        input, result, proc = self.startchildproc(processWorker)

        for myfile in dicttocopy:
            fileid = myfile['pfn'].split('/')[-1]

            dirpath = os.path.join(self.options.destination, myfile['suffix'] if 'suffix' in myfile else '')
            if not os.path.isdir(dirpath):
                os.makedirs(dirpath)
            localFilename = os.path.join(dirpath,  str(fileid))

            ##### Handling the "already existing file" use case
            if os.path.isfile(localFilename):
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
            if os.path.isfile(localFilename):
                if not sys.stdout.isatty():
                    self.logger.info("Skipping %s as %s already exists" % ( fileid, localFilename, ))
                else:
                    self.logger.info("%sSkipping %s as %s already exists%s" % (colors.GREEN, fileid, localFilename, colors.NORMAL))
                continue

            ##### Creating the command
            maxtime = srmtimeout if not 'size' in myfile or myfile['size']==0 else int(ceil(2*myfile['size']/downspeed)) #timeout based on file size and download speed * 2
            localsrmtimeout = minsrmtimeout if maxtime < minsrmtimeout else maxtime #do not want a too short timeout
            cmd = '%s %s %s file://%s' % (lcgCmd, ' --srm-timeout ' + str(localsrmtimeout) + ' ', myfile['pfn'], localFilename)

            self.logger.info("Retrieving file '%s' " % fileid)
            self.logger.debug("Executing '%s' " % cmd)
            input.put((fileid, cmd))
            starttime = time.time()
            endtime = 0
            res = None
            stdout   = ''
            stderr   = ''
            exitcode = -1
            try:
                res = result.get(block = True, timeout = lcgtimeout+localsrmtimeout)
                self.logger.debug("Command finished")
                endtime = time.time()
                stdout   = res['stdout']
                stderr   = res['stderr']
                exitcode = res['exit']
            except Queue.Empty:
                self.logger.debug("Command timed out")
                stderr   = "Timeout retrieving result after %i seconds" % (lcgtimeout+localsrmtimeout)
                stdout   = ''
                exitcode = -1
                downspeed -= downspeed*0.5 #if fails for timeout, reducing download bandwidth of 50%

            checkout = simpleOutputCheck(stdout)
            checkerr = simpleOutputCheck(stderr)
            checksumOK = False
            if hasattr(myfile, 'checksum'):
                self.logger.debug("Checksum '%s'" %str(myfile['checksum']))
                checksumOK = checksumChecker(localFilename, myfile['checksum'])
            else:
                checksumOK = True # No checksums provided

            if exitcode is not 0 or (len(checkout) + len(checkerr)) > 0:
                ## check to track srmv1 issues, probably this is strong enough to find all of them
                ## REMOVE this check as soon as sites will have switched to srmv2
                if ('srmv1' in myfile['pfn'] or 'managerv1' in myfile['pfn']) and len( filter(lambda elem: elem.find('communication error on send')!=-1, checkerr) ) > 0:
                    msgFail  = '\n\tThe site storage is using srmv1, which is deprecated and not anymore supported.\n'
                    msgFail += '\tPlease report this issue with the PFN provided here below.\n\tPFN: "%s".' % str(myfile['pfn'])
                    finalresults[fileid] = {'exit': False, 'error': msgFail, 'dest': None}
                else:
                    if 'timeout' in stdout or 'timeout' in stderr or 'timed out' in stdout or 'timed out' in stderr:
                        downspeed -= downspeed*0.5 #if fails for timeout, reducing download bandwidth of 50%
                    finalresults[fileid] = {'exit': False, 'output': checkout, 'error' : checkerr, 'dest': None}

                if not sys.stdout.isatty():
                    self.logger.info("Failed retrieving file %s" % fileid)
                else:
                    self.logger.info(colors.RED + "Failed retrieving file %s" % fileid + colors.NORMAL)

                if len(finalresults[fileid]['output']) > 0:
                    self.logger.info("Output:")
                    [self.logger.info("\t %s" % x) for x in finalresults[fileid]['output']]
                if len(finalresults[fileid]['error']) > 0:
                    self.logger.info("Error:")
                    [self.logger.info("\t %s" % x) for x in finalresults[fileid]['error']]
            elif not checksumOK:
                msg = "Checksum failed for job " + str(fileid)
                finalresults[fileid] = {'exit': False, 'error': msg, 'dest': None}
                self.logger.info( msg )
            else:
                finalresults[fileid] = {'exit': True, 'dest': os.path.join(dirpath, str(fileid)), 'error': None}
                if not sys.stdout.isatty():
                    self.logger.info("Successfully retrived file %s" % fileid)
                else:
                    self.logger.info(colors.GREEN + "Successfully retrived file %s" % fileid + colors.NORMAL)
                tottime = endtime - starttime
                downspeed = myfile['size']/tottime #calculating average of download bandwidth during last copy
                self.logger.debug("Transfer took %.1f sec. and average speed of %.1f KB/s" % (tottime, downspeed/1024))

        self.stopchildproc(input, proc)

        for fileid in finalresults:
            if finalresults[fileid]['exit']:
                self.logger.info("File %s has been placed in %s" %(fileid, finalresults[fileid]['dest']))
            else:
                self.logger.debug(str(finalresults[fileid]))
                self.logger.debug("File %s: transfer problem %s" %(fileid, str(finalresults[fileid]['error'])))
                globalExitcode = 1

        if len(finalresults.keys()) is 0:
            self.logger.info("Nothing has been retrieved.")
        else:
            self.logger.info("Retrieval completed")

        if globalExitcode == -1:
            globalExitcode = 0
        return CommandResult(globalExitcode, '')

    def startchildproc(self, childprocess):
        """
        starting sub process and creating the queue
        """
        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        p = multiprocessing.Process(target = childprocess, args = (input, result))
        p.start()
        return input, result, p

    def stopchildproc(self, inqueue, childprocess):
        """
        simply sending a STOP message to the sub process
        """
        try:
            inqueue.put( ('-1', 'STOP', 'control') )
        except Exception, ex:
            pass
        finally:
            # giving the time to the sub-process to exit
            childprocess.terminate()
            time.sleep(1)


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


import time, fcntl, select,signal
from subprocess import Popen, PIPE, STDOUT

from os import kill
from signal import alarm, signal, SIGALRM, SIGKILL
from subprocess import PIPE, Popen


def processWorker(input, results):
    """
    _processWorker_

    Runs a subprocessed command.
    """

    # Get this started
    t1 = None
    jsout = None

    while True:
        workid = None
        try:
            pfn, work = input.get()
            t1 = time.time()
        except (EOFError, IOError):
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt."
            print crashMessage
            break

        if work == 'STOP':
            break

        command = work
        pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                                 stderr = subprocess.PIPE, shell = True)
        stdout, stderr = pipe.communicate()

        results.put( {
                       'pfn': pfn,
                       'stdout': stdout,
                       'stderr': stderr,
                       'exit':   pipe.returncode
                     })

    return 0

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
