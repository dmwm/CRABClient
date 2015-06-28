from __future__ import division
import os
import subprocess
import multiprocessing, Queue
import time
import re
from math import ceil
from multiprocessing import Manager

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.ClientUtilities import colors, cmd_exist


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

    def __call__(self):
        """
        Copying locally files staged remotely.
         *Using a subprocess to encapsulate the copy command.
         * maximum parallel download is 10, line 61
         * default --sendreceive-timeout is 1800 s, line 75 and 77
        """

        globalExitcode = -1

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
        if cmd_exist("gfal-copy"):
            self.logger.info("Will use `gfal-copy` command for file transfers")
            command = "env -i X509_USER_PROXY=%s gfal-copy -v " % os.path.abspath(self.proxyfilename)
            command += " -T "
        elif cmd_exist("lcg-cp"):
            self.logger.info("Will use `lcg-cp` command for file transfers")
            command = "lcg-cp --connect-timeout 20 --verbose -b -D srmv2"
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

                # delete the file if its size is zero or its size is not the expected size
                if size == 0 or ('size' in myfile and myfile['size'] != size):
                    try:
                        self.logger.info("Removing %s as it is not complete: current size %s, expected size %s" % (fileid, size, \
                                                                                myfile['size'] if 'size' in myfile else 'unknown'))
                        os.remove(localFilename)
                    except Exception as ex:
                        self.logger.info("%sError%s: Cannot remove the file because of: %s" % (colors.RED, colors.NORMAL,ex))

            # if the file still exists skip it
            if not url_input and os.path.isfile(localFilename):
                self.logger.info("Skipping %s as file already exists in %s" % (fileid, localFilename))
                continue

            ##### Creating the command
            # timeout based on file size and download speed * 2
            maxtime = srmtimeout if not 'size' in myfile or myfile['size'] == 0 else int(ceil(2*myfile['size']/downspeed))
            localsrmtimeout = minsrmtimeout if maxtime < minsrmtimeout else maxtime # do not want a too short timeout

            timeout = " --srm-timeout "
            if cmd_exist("gfal-copy"):
                timeout = " -t "
            cmd = '%s %s %s %%s' % (command, timeout + str(localsrmtimeout) + ' ', myfile['pfn'])
            if url_input:
                cmd = cmd % localFilename
            else:
                cmd = cmd % ("file://%s" % localFilename)

            self.logger.info("Placing file '%s' in retrieval queue " % fileid)
            inputq.put((myfile, cmd))

        self.logger.info("Please wait")
        self.stopchildproc(inputq, processarray,nsubprocess)


        #getting output for global exit
        if len(successfiles) == 0:
            self.logger.info("No file retrieved")
            globalExitcode = -1
        elif len(failedfiles) != 0:
            self.logger.info(colors.GREEN+"Number of files successfully retrieved: %s" % len(successfiles)+colors.NORMAL)
            self.logger.info(colors.RED+"Number of files failed to be retrieved: %s" % len(failedfiles)+colors.NORMAL)
            #self.logger.debug("List of failed file and reason: %s" % failedfiles)
            globalExitcode = -1
        else:
            self.logger.info("%sSuccess%s: All files successfully retrieved" % (colors.GREEN,colors.NORMAL))
            globalExitcode = 0

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
                url_input = bool(re.match("^[a-z]+://", dirpath))
                if not os.path.isdir(dirpath) and not url_input:
                    os.makedirs(dirpath)
                localFilename = os.path.join(dirpath,  str(fileid))
                command = work

            self.logger.info("Retrieving %s " % fileid)
            self.logger.debug("Executing %s" % command)
            pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                                     stderr = subprocess.PIPE, shell = True)
            stdout, stderr = pipe.communicate()
            error = simpleOutputCheck(stderr)

            self.logger.debug("Finish executing for file %s" % fileid)

            if pipe.returncode != 0 or len(error) > 0:
                self.logger.info("%sWarning%s: Failed retrieving %s" % (colors.RED, colors.NORMAL, fileid))
                #self.logger.debug(colors.RED +"Stderr: %s " %stderr+ colors.NORMAL)
                [self.logger.debug(colors.RED +"\t %s" % x + colors.NORMAL) for x in error]
                failedfiles[fileid] = str(error)

                if "timed out" in stderr or "timed out" in stdout:
                    self.logger.info("%sWarning%s: Failed due to connection timeout" % (colors.RED, colors.NORMAL ))
                    self.logger.info("Please use the '-w' option to increase the connection timeout")

                if os.path.isfile(localFilename) and os.path.getsize(localFilename) != myfile['size']:
                    self.logger.debug("File %s has the wrong size, deleting it" % fileid)
                    try:
                       os.remove(localFilename)
                    except Exception as ex:
                        self.logger.debug("%sWarning%s: Cannot remove the file because of: %s" % (colors.RED, colors.NORMAL, ex))
                time.sleep(60)
                continue
            else:
                self.logger.info("%sSuccess%s: Success in retrieving %s " % (colors.GREEN, colors.NORMAL, fileid))
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
