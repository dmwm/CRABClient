from CRABClient.Commands import CommandResult
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.CredentialInteractions import CredentialInteractions
from WMCore.Credential.Proxy import CredentialException
from CRABClient.client_utilities import initProxy
import os
import logging
import subprocess
import threading
import multiprocessing, Queue
import time

from WMCore.FwkJobReport.FileInfo import readAdler32, readCksum
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

        self.parser.add_option( "-r", "--role",
                                dest = "role",
                                default = None )

        self.parser.add_option( "-g", "--group",
                                dest = "group",
                                default = None )


    def __call__(self):
        globalExitcode = -1

        dicttocopy = self.options.inputdict

        if not self.options.skipProxy:
            _, self.proxyfilename = initProxy( self.options.role, self.options.group, self.logger)
        else:
            self.logger.debug('Skipping proxy creation and delegation')

        lcgCmd = 'lcg-cp --connect-timeout 20 --sendreceive-timeout 240 --srm-timeout 1800 --verbose -b -D srmv2'

        finalresults = {}

        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()

        singletimeout = 35 * 60
        p = multiprocessing.Process(target = processWorker, args = (input, result))
        p.start()

        ## this can be parallelized starting more processes
        for file in dicttocopy:
            fileid = file['pfn'].split('/')[-1]
            localFilename = os.path.join(self.options.destination, str(fileid))
            cmd = '%s %s file://%s' % (lcgCmd, file['pfn'], localFilename)
            self.logger.info("Retrieving file '%s' " % fileid)
            self.logger.debug("Executing '%s' " % cmd)
            input.put((fileid, cmd))

            res = None
            stdout   = ''
            stderr   = ''
            exitcode = -1
            try:
                res = result.get(block = True, timeout = singletimeout)
                stdout   = res['stdout']
                stderr   = res['stderr']
                exitcode = res['exit']
            except Queue.Empty:
                stderr   = "Timeout retrieving result after %i seconds" % singletimeout
                stdout   = ''
                exitcode = -1

            self.logger.debug("Verify command result")

            checkout = simpleOutputCheck(stdout)
            checkerr = simpleOutputCheck(stderr)
            checksumOK = False
            if hasattr(file, 'checksum'):
                self.logger.debug("Checksum '%s'" %str(file['checksum']))
                checksumOK = checksumChecker(localFilename, file['checksum'])
            else:
                checksumOK = True # No checksums provided

            if exitcode is not 0 or (len(checkout) + len(checkerr)) > 0:
                ## check to track srmv1 issues, probably this is strong enough to find all of them
                ## REMOVE this check as soon as sites will have switched to srmv2
                if ('srmv1' in file['pfn'] or 'managerv1' in file['pfn']) and len( filter(lambda elem: elem.find('communication error on send')!=-1, checkerr) ) > 0:
                    msgFail  = '\n\tThe site storage is using srmv1, which is deprecated and not anymore supported.\n'
                    msgFail += '\tPlease report this issue with the PFN provided here below.\n\tPFN: "%s".' % str(file['pfn'])
                    finalresults[fileid] = {'exit': False, 'error': msgFail, 'dest': None}
                else:
                    finalresults[fileid] = {'exit': False, 'output': checkout, 'error' : checkerr, 'dest': None}
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
                finalresults[fileid] = {'exit': True, 'dest': os.path.join(self.options.destination, str(fileid)), 'error': None}
                self.logger.info(colors.GREEN + "Successfully retrived file %s" % fileid + colors.NORMAL)

        try:
            input.put( ('-1', 'STOP', 'control') )
        except Exception, ex:
            pass
        finally:
            # giving the time to the sub-process to exit
            p.terminate()
            time.sleep(1)

        for fileid in finalresults:
            if finalresults[fileid]['exit']:
                self.logger.info("File %s has been placed in %s" %(fileid, finalresults[fileid]['dest']))
            else:
                self.logger.debug(str(finalresults[fileid]))
                self.logger.debug("File %s: transfer problem %s" %(fileid, str(finalresults[fileid]['error'])))
                globalExitcode = 1

        if len(finalresults.keys()) is 0:
            self.logger.info("Nothing to retrieve.")
        else:
            self.logger.info("Retrieval completed")

        if globalExitcode == -1:
            globalExitcode = 0
        return CommandResult(globalExitcode, '')


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


    return problems


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
