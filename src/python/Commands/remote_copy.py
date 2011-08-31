from Commands import CommandResult
from Commands.SubCommand import SubCommand
from CredentialInteractions import CredentialInteractions
from WMCore.Credential.Proxy import CredentialException
from client_utilities import initProxy
import os
import operator
import logging
import subprocess
import threading
import multiprocessing, Queue
import time

from WMCore.FwkJobReport.FileInfo import readAdler32, readCksum

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

        self.parser.add_option( "-e", "--extension",
                                dest = "extension",
                                default = 'root' )

        self.parser.add_option( "-p", "--skip-proxy",
                                action = "store_true",
                                dest = "skipProxy",
                                default = None,
                                help = "Skip Grid proxy creation and myproxy delegation")


    def __call__(self):
        globalExitcode = -1

        dicttocopy = self.options.inputdict

        if not self.options.skipProxy:
            try:
                initProxy( None, None, self.options.role, self.options.group, False, self.logger)
            except CredentialException, ce:
                msg = "Problem during proxy creation: \n %s " % str(ce._message)
                return CommandResult(1, msg)
        else:
            logging.debug('Skipping proxy creation and delegation')

        lcgCmd = 'lcg-cp --connect-timeout 20 --sendreceive-timeout 240 --srm-timeout 2400 --verbose -b -D srmv2'

        sortedbyjob = sorted(dicttocopy.iteritems(), key = operator.itemgetter(1))
        finalresults = {}

        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()

        singletimeout = 20 * 60
        p = multiprocessing.Process(target = processWorker, args = (input, result))
        p.start()

        self.logger.info("Starting retrieving remote files for requested jobs %s " % str([s.encode('utf-8') for s in dicttocopy.keys()]))
        ## this can be parallelized starting more processes
        for jobid, lfn in sortedbyjob:
            self.logger.debug("Processing job %s" % jobid)
            localFilename = os.path.join(self.options.destination, jobid + '.' + self.options.extension)
            cmd = '%s %s file://%s' % (lcgCmd, lfn['pfn'], localFilename)
            self.logger.debug("Executing '%s' " % cmd)
            input.put((int(jobid), cmd, ''))

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
                stderr   = "Timeout retrieving result after %i" % singletimeout
                stdout   = ''
                exitcode = -1

            self.logger.debug("Processed job %s" % jobid)
            self.logger.debug("Verify command result %s" % jobid)

            checkout = simpleOutputCheck(stdout)
            checkerr = simpleOutputCheck(stderr)

            checksumOK = False
            if hasattr(lfn, 'checksums'):
                self.logger.debug("Checksum '%s'" %str(lfn['checksums']))
                checksumOK = checksumChecker(localFilename, lfn['checksums'])
            else:
                checksumOK = True # No checksums provided

            if exitcode is not 0 or (len(checkout) + len(checkerr)) > 0:
                finalresults[jobid] = {'exit': False, 'lfn': lfn, 'error': checkout + checkerr, 'dest': None}
                self.logger.debug("Failed retrieving job %s" % jobid)
            elif not checksumOK:
                msg = "Checksum failed for job " + str(jobid)
                finalresults[jobid] = {'exit': False, 'lfn': lfn, 'error': msg, 'dest': None}
                self.logger.debug( msg )
            else:
                finalresults[jobid] = {'exit': True, 'lfn': lfn, 'dest': os.path.join(self.options.destination, str(jobid) + '.' + self.options.extension), 'error': None}
                self.logger.debug("Retrived job, checksum passed %s" % jobid)

        try:
            input.put( ('-1', 'STOP', 'control') )
        except Exception, ex:
            pass
        finally:
            # giving the time to the sub-process to exit
            p.terminate()
            time.sleep(1)

        for jobid in finalresults:
            if finalresults[jobid]['exit']:
                self.logger.info("Job %s: output in %s" %(jobid, finalresults[jobid]['dest']))
            else:
                self.logger.debug(str(finalresults[jobid]))
                self.logger.info("Job %s: transfer problem %s" %(jobid, str(finalresults[jobid]['error'])))
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
        elif line.find("timeout") != -1:
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
        type = ''
        workid = None
        try:
            workid, work, type = input.get()
            t1 = time.time()
        except (EOFError, IOError):
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            print crashMessage
            break

        if work == 'STOP':
            break

        command = work
        pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                                 stderr = subprocess.PIPE, shell = True)
        stdout, stderr = pipe.communicate()

        results.put( {
                       'workid': workid,
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
