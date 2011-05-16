from Commands import CommandResult
from Commands.SubCommand import SubCommand 
from CredentialInteractions import CredentialInteractions
import os
import operator
import logging
import subprocess
import threading
import multiprocessing
import time


class remote_copy(SubCommand):

    ## currently doesn't need to be showed to the outside
    visible = False

    ## name should become automatically generated
    name  = "remote_copy"
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


    def __call__(self, args):

        (options, args) = self.parser.parse_args( args )

        dicttocopy = options.inputdict
        # If I'm copying I need to deal with proxies
        # serverdn, myproxy, role, group, logger
        proxy = CredentialInteractions( None, None, options.role, options.group, self.logger )

        self.logger.info("Checking credentials")
        proxy.createNewVomsProxy( timeleftthreshold = 600 )
        #self.logger.info("Registering user credentials")
        #proxy.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * 3)

        command = 'lcg-cp --connect-timeout 20 --sendreceive-timeout 240 --bdii-timeout 20 --srm-timeout 2400 --verbose'

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
            self.logger.debug("Processing job %s" % str(jobid))
            cmd = command + ' ' + lfn + ' file://' + os.path.join(options.destination, str(jobid) + '.' + options.extension)
            input.put((jobid, cmd, ''))

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
                msg = "Timeout retrieving result after %i" % singletimeout
                stdout   = ''
                stderr   = msg
                exitcode = -1
  
            self.logger.debug("Processed job %s" % str(jobid))
            self.logger.debug("Verify command result %s" % str(jobid))

            checkout = simpleOutputCheck(stdout)
            checkerr = simpleOutputCheck(stderr)
            if exitcode is not 0 or (len(checkout) + len(checkerr)) > 0:
                finalresults[jobid] = {'exit': False, 'lfn': lfn, 'error': checkout + checkerr, 'dest': None}
                self.logger.debug("Failed retriving job %s" % str(jobid))
            else:
                finalresults[jobid] = {'exit': True, 'lfn': lfn, 'dest': os.path.join(options.destination, str(jobid) + '.root'), 'error': None} 
                self.logger.debug("Retrived job %s" % str(jobid))

        try:
            input.put( ('-1', 'STOP', 'control') )
        except Exception, ex:
            pass 
        finally:
            p.terminate()

        self.logger.debug(str(finalresults))
        for jobid in finalresults:
            if finalresults[jobid]['exit']:
                self.logger.info("Job %s: output in %s" %(jobid, finalresults[jobid]['dest']))
            else:
                self.logger.info("Job %s: transfer problem %s" %(jobid, str(finalresults[jobid]['error'])))

        if len(finalresults.keys()) is 0:
            self.logger.info("Nothing to retrieve.")
        else:
            self.logger.info("Retrieval completed")

        return CommandResult(0, None)


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


