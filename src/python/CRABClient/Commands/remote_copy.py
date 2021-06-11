from __future__ import division
from __future__ import print_function
import os
import subprocess
import multiprocessing
import time
import re
from math import ceil
import logging
from multiprocessing import Manager

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import colors, cmd_exist, logfilter, execute_command


class remote_copy(SubCommand):

    ## currently doesn't need to be showed to the outside
    visible = False

    name  = __name__.split('.').pop()
    usage = "usage: %prog " + name + " [options] [args]"

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        self.remotecpLogile = None


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option("--destination",
                               dest = "destination",
                               default = None )

        self.parser.add_option("--input",
                               dest = "inputdict",
                               default = None )

        self.parser.add_option("--parallel",
                               dest = "nparallel")

        self.parser.add_option("--wait",
                               dest = "waittime")

        self.parser.add_option("--checksum",
                               dest = "checksum")

        self.parser.add_option("--command",
                               dest = "command")


    def __call__(self):
        """
        Copying locally files staged remotely.
         * using a subprocess to encapsulate the copy command.
         * maximum parallel download is 10
        """
        ## This is the log gilename that is going to be used by the subprocesses that copy the file
        ## Using the same logfile is not supported automatically, see:
        ## https://docs.python.org/2/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
        self.remotecpLogile = "%s/remote_copy.log" % os.path.dirname(self.logger.logfile)
        dicttocopy = self.options.inputdict

        # taking number of parallel download to create from user, default is 10
        if self.options.nparallel == None:
            nsubprocess = 10
        else:
            nsubprocess = int(self.options.nparallel)

        if nsubprocess <= 0 or nsubprocess > 20:
            self.logger.info("Inappropriate number of parallel download, must between 0 to 20 ")
            return -1
        command = ""
        if cmd_exist("gfal-copy") and self.options.command not in ["LCG"]:
            self.logger.info("Will use `gfal-copy` command for file transfers")
            command = "gfal-copy -v "
            if self.options.checksum:
                command += "-K %s " % self.options.checksum
            command += " -T "
        elif cmd_exist("lcg-cp") and self.options.command not in ["GFAL"]:
            self.logger.info("Will use `lcg-cp` command for file transfers")
            command = "lcg-cp --connect-timeout 20 --verbose -b -D srmv2"
            if self.options.checksum:
                command += " --checksum-type %s " % self.options.checksum
            command += " --sendreceive-timeout "
        else:
            # This should not happen. If it happens, Site Admin have to install GFAL2 (yum install gfal2-util gfal2-all)
            self.logger.info("%sError%s: Can`t find command `gfal-copy` or `lcg-ls`, Please contact the site administrator." % (colors.RED, colors.NORMAL))
            return [], []

        command += "1800" if self.options.waittime == None else str(1800 + int(self.options.waittime))

        # timeout = 20 + 240 + 60 #giving 1 extra minute: 5min20"
        srmtimeout = 900 # default transfer timeout in case the file size is unknown: 15min
        minsrmtimeout = 60 # timeout cannot be less then 1min
        downspeed = float(250*1024) # default speed assumes a download of 250KB/s
        mindownspeed = 20*1024.

        manager = Manager()
        successfiles = manager.dict()
        failedfiles = manager.dict()


        self.logger.debug("Starting ChildProcess with %s ChildProcess" % nsubprocess)
        inputq, processarray = self.startchildproc(self.processWorker, nsubprocess, successfiles, failedfiles)

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

                # delete the file if its size is zero or its size is not the expected size
                if size == 0 or ('size' in myfile and myfile['size'] != size):
                    try:
                        self.logger.info("Removing %s as it is not complete: current size %s, expected size %s" % (fileid, size, \
                                                                                myfile['size'] if 'size' in myfile else 'unknown'))
                        os.remove(localFilename)
                    except OSError as ex:
                        self.logger.info("%sError%s: Cannot remove the file because of: %s" % (colors.RED, colors.NORMAL, ex))

            # if the file still exists skip it
            if not url_input and os.path.isfile(localFilename):
                self.logger.info("Skipping %s as file already exists in %s" % (fileid, localFilename))
                continue

            ##### Creating the command
            # better to execut grid commands in the pre-CMS environment
            undoScram = "which scram >/dev/null 2>&1 && eval `scram unsetenv -sh`"

            # timeout based on file size and download speed * 2
            maxtime = srmtimeout if not 'size' in myfile or myfile['size'] == 0 else int(ceil(2*myfile['size']/downspeed))
            localsrmtimeout = minsrmtimeout if maxtime < minsrmtimeout else maxtime # do not want a too short timeout
            timeout = " --srm-timeout "
            if cmd_exist("gfal-copy") and self.options.command not in ["LCG"]:
                timeout = " -t "
            cmd = undoScram + '; %s %s %s %%s' % (command, timeout + str(localsrmtimeout) + ' ', myfile['pfn'])
            if url_input:
                cmd = cmd % localFilename
            else:
                cmd = cmd % ("file://%s" % localFilename)

            self.logger.info("Placing file '%s' in retrieval queue " % fileid)
            inputq.put((myfile, cmd))

        self.logger.info("Please wait")

        keybInt = self.stopchildproc(inputq, processarray, nsubprocess)

        self.saveSubprocessesOut(failedfiles, keybInt)

        if keybInt:
            ## if ctrl-C was hit we wont find anything interesting in the subprocesses out
            ## that means that successfiles and failedfiles will not be dict as normally expected
            return [], []
        elif len(successfiles) == 0:
            self.logger.info("No file retrieved")
        elif len(failedfiles) != 0:
            self.logger.info(colors.GREEN+"Number of files successfully retrieved: %s" % len(successfiles)+colors.NORMAL)
            self.logger.info(colors.RED+"Number of files failed to be retrieved: %s" % len(failedfiles)+colors.NORMAL)
            #self.logger.debug("List of failed file and reason: %s" % failedfiles)
        else:
            self.logger.info("%sSuccess%s: All files successfully retrieved" % (colors.GREEN,colors.NORMAL))

        return successfiles , failedfiles

    def startchildproc(self, childprocess, nsubprocess, successfiles, failedfiles):
        """
        starting sub process and creating the queue
        """
        inputq  = multiprocessing.Queue()
        subprocessarray = []

        for i in xrange(nsubprocess):
            p = multiprocessing.Process(target = childprocess, args = (inputq, successfiles, failedfiles))
            subprocessarray.append(p)
            subprocessarray[i].start()

        return inputq, subprocessarray


    def stopchildproc(self, inputq, processarray, nsubprocess):
        """ Simply sending a STOP message to the sub process
            Return True if ctrl-C has been hit
        """
        result = False #using variable instead of direct return to make pylint happy. See W0150
        self.logger.debug("stopchildproc() method has been called")
        try:
            for _ in range(nsubprocess):
                inputq.put(('-1', 'STOP'))
        #except Exception, ex:
        #   pass
        finally:
            # giving the time to the sub-process to exit
            for process in processarray:
                try:
                    process.join()
                except KeyboardInterrupt:
                    self.logger.info("Master process keyboard interrupted while waiting")
                    result = True
        return result

    def saveSubprocessesOut(self, failedfiles, keybInt):
        """ Get the logfile produced by the subprocesses and put it into
            the usual crab.log file
        """
        if os.path.isfile(self.remotecpLogile):
            self.logger.debug("The output of the transfer subprocesses follows:")
            with open(self.remotecpLogile) as fp:
                for line in fp:
                    self.logger.debug("\t" + line[:-1]) #-1 is to remove the newline at the end

            os.remove(self.remotecpLogile)

            if keybInt or failedfiles: # N.B. failed files cannot be read if keybInt
                self.logger.info("For more details about the errors please open the logfile")
        else:
            self.logger.debug("Cannot find %s" % self.remotecpLogile)

    def setSubprocessLog(self):
        """ Set the logger for the subprocess workers so that everything that
            is debug get logged to a file, and everything that is info get logged
            to both screen and file.

            See https://docs.python.org/2/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
            for the reason we need to do that.

            The file will be put into the "main" crab.log file and removed by
            the master process by saveSubprocessesOut
        """
        logger = logging.getLogger('remotecopy')
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s %(asctime)s: \t %(message)s')
        consolehandler = logging.StreamHandler()
        consolehandler.setLevel(logging.INFO) #            consolehandler.setFormatter(formatter)
        logger.addHandler(consolehandler)
        filehandler = logging.FileHandler(self.remotecpLogile)
        filehandler.setFormatter(formatter)
        filehandler.setLevel(logging.DEBUG)
        filehandler.addFilter(logfilter())
        logger.addHandler(filehandler)
        return logger


    def processWorker(self, input_, successfiles, failedfiles):
        """
        _processWorker_

        Runs a subprocessed command.
        """
        logger = self.setSubprocessLog()
        # Get this started
        while True:
            try:
                myfile, work = input_.get()
            except (EOFError, IOError):
                crashMessage = "Hit EOF/IO in getting new work\n"
                crashMessage += "Assuming this is a graceful break attempt."
                print(crashMessage)
                break

            if work == 'STOP':
                break
            else:
                fileid = myfile['pfn'].split('/')[-1]
                dirpath = os.path.join(self.options.destination, myfile['suffix'] if 'suffix' in myfile else '')
                url_input = bool(re.match("^[a-z]+://", dirpath))
                if not os.path.isdir(dirpath) and not url_input:
                    os.makedirs(dirpath)
                localFilename = os.path.join(dirpath,  str(fileid))
                command = work

            logger.info("Retrieving %s " % fileid)
            logger.debug("Executing %s" % command)
            try:
                stdout, stderr, returncode = execute_command(command=command)
            except KeyboardInterrupt:
                logger.info("Subprocess exit due to keyboard interrupt")
                break
            error = simpleOutputCheck(stderr)

            logger.debug("Finish executing for file %s" % fileid)

            if returncode != 0 or len(error) > 0:
                logger.info("%sWarning%s: Failed retrieving %s" % (colors.RED, colors.NORMAL, fileid))
                #logger.debug(colors.RED +"Stderr: %s " %stderr+ colors.NORMAL)
                for x in error:
                    logger.info(colors.RED +"\t %s" % x + colors.NORMAL)
                failedfiles[fileid] = str(error)
                logger.debug("Full stderr follows:\n%s" % stderr)

                if "timed out" in stderr or "timed out" in stdout:
                    logger.info("%sWarning%s: Failed due to connection timeout" % (colors.RED, colors.NORMAL ))
                    logger.info("Please use the '--wait=<#seconds>' option to increase the connection timeout")

                if "checksum" in stderr:
                    logger.info("%sWarning%s: as of 3.3.1510 CRAB3 is using an option to validate the checksum with lcg-cp/gfal-cp commands."
                                " You might get false positives since for some site this is not working."
                                " In that case please use the option --checksum=no"% (colors.RED, colors.NORMAL ))

                if os.path.isfile(localFilename) and os.path.getsize(localFilename) != myfile['size']:
                    logger.debug("File %s has the wrong size, deleting it" % fileid)
                    try:
                        os.remove(localFilename)
                    except OSError as ex:
                        logger.debug("%sWarning%s: Cannot remove the file because of: %s" % (colors.RED, colors.NORMAL, ex))
                try:
                    time.sleep(60)
                except KeyboardInterrupt:
                    logger.info("Subprocess exit due to keyboard interrupt")
                    break
            else:
                logger.info("%sSuccess%s: Success in retrieving %s " % (colors.GREEN, colors.NORMAL, fileid))
                successfiles[fileid] = 'Successfully retrieved'
        return


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
