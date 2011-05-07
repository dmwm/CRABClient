from Commands import CommandResult
from CredentialInteractions import CredentialInteractions
import os
import operator
import logging
import subprocess
import threading
import multiprocessing
import time


def remote_copy(logger, configuration, server, options, requestname, requestareai):
    """
    """

    # If I'm submitting I need to deal with proxies
    proxy = CredentialInteractions(
                                    configuration.General.serverdn,
                                    configuration.General.myproxy,
                                    getattr(configuration.User, "vorole", ""),
                                    getattr(configuration.User, "vogroup", ""),
                                    logger
                                  )

    logger.info("Checking credentials")
    proxy.createNewVomsProxy( timeleftthreshold = 600 )
    #logger.info("Registering user credentials")
    #proxy.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * 3)

    ## this can be replaced by a mapping dictionary to support multiple commands/protocols
    command = 'lcg-cp --connect-timeout 20 --sendreceive-timeout 240 --bdii-timeout 20 --srm-timeout 2400 --verbose'

    sortedbyjob = sorted(options['inputdict'].iteritems(), key = operator.itemgetter(1))
    finalresults = {}

    input  = multiprocessing.Queue()
    result = multiprocessing.Queue()

    singletimeout = 20 * 60
    p = multiprocessing.Process(target = processWorker, args = (input, result))
    p.start()

    logger.info("Starting retrieving output for requested jobs %s " % str([s.encode('utf-8') for s in options['inputdict'].keys()]))
    ## this can be parallelized starting more processes
    for jobid, lfn in sortedbyjob:
        logger.debug("Processing job %s" % str(jobid))
        cmd = command + ' ' + lfn + ' file://' + os.path.join(options['dest'], str(jobid) + '.root')
        input.put((jobid, cmd, ''))

        res = None
        stdout   = ''
        stderr   = ''
        exitcode = -1
        try:
            res = result.get(block = True, timeout = singletimeout)
        except multiprocessing.Queue.Empty:
            msg = "Timeout retrieving result after %i" % singletimeout
            stdout   = ''
            stderr   = msg
            exitcode = -1

        logger.debug("Processed job %s" % str(jobid))
        logger.debug("Verify command result %s" % str(jobid))

        stdout   = res['stdout']
        stderr   = res['stderr']
        exitcode = res['exit']

        ## replace the std-out/err with checksum check
        checkout = simpleOutputCheck(stdout)
        checkerr = simpleOutputCheck(stderr)
        if exitcode is not 0 or (len(checkout) + len(checkerr)) > 0:
            finalresults[jobid] = {'exit': False, 'lfn': lfn, 'error': checkout + checkerr, 'dest': None}
            logger.debug("Failed retriving job %s" % str(jobid))
        else:
            finalresults[jobid] = {'exit': True, 'lfn': lfn, 'dest': os.path.join(options['dest'], str(jobid) + '.root'), 'error': None} 
            logger.debug("Retrived job %s" % str(jobid))

    try:
        input.put( ('-1', 'STOP', 'control') )
    except Exception, ex:
        pass 
    finally:
        p.terminate()

    logger.debug(str(finalresults))
    for jobid in finalresults:
        if finalresults[jobid]['exit']:
            logger.info("Job %s: output in %s" %(jobid, finalresults[jobid]['dest']))
        else:
            logger.info("Job %s: transfer problem %s" %(jobid, str(finalresults[jobid]['error'])))
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


