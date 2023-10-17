import os
import tempfile

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.UserUtilities import getUsername
from CRABClient.ClientUtilities import execute_command, colors
from CRABClient.ClientExceptions import MissingOptionException

class checkdataset(SubCommand):
    """
    check availability on disk of a dataset
    """
    name = 'checkdataset'
    shortnames = ['chkd']

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        self.dataset = None


    def __call__(self):
        #import pdb; pdb.set_trace()

        if hasattr(self.options, 'dataset') and self.options.dataset:
            dataset = self.options.dataset

        username = getUsername(self.proxyfilename, logger=self.logger)
        tmpDir = tempfile.mkdtemp()
        scriptName = "checkDiskAvailability.py"

        # download up-to-date version of the script
        source = "https://github.com/dmwm/CRABServer/raw/master/scripts/Utils/CheckDiskAvailability.py"
        cmd = "cd %s; " % tmpDir
        cmd += "/usr/bin/wget --output-document=%s --timeout=60 %s" % (scriptName, source)
        # put it in a script in order
        out, err, exitcode = execute_command(cmd)
        if exitcode:
            self.logger.info("Failed to download script from GitHub. Please try later")
            if out:
                self.logger.info('  Stdout:\n    %s' % str(out).replace('\n', '\n    '))
            if err:
                self.logger.info('  Stderr:\n    %s' % str(err).replace('\n', '\n    '))
            return {'commandStatus': 'FAILED'}

        # execute it in the Rucio environment
        cmd = 'eval `scram unsetenv -sh`; '
        cmd += 'source /cvmfs/cms.cern.ch/rucio/setup-py3.sh >/dev/null; '
        cmd += 'export RUCIO_ACCOUNT=%s; ' % username
        cmd += 'cd %s; ' % tmpDir
        cmd += 'python3 %s --dataset %s; ' % (scriptName, dataset)
        out, err, exitcode = execute_command(cmd)
        if exitcode:
            self.logger.info("Failed to execute CheckDatasetAvailability.py. Contact support")
            if out:
                self.logger.info('  Stdout:\n    %s' % str(out).replace('\n', '\n    '))
            if err:
                self.logger.info('  Stderr:\n    %s' % str(err).replace('\n', '\n    '))
            return {'commandStatus': 'FAILED'}
        else:
            self.logger.info(out)
        os.unlink(os.path.join(tmpDir, scriptName))
        os.rmdir(tmpDir)
        return {'commandStatus': 'SUCCESS'}


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--dataset',
                               dest='dataset',
                               default=None,
                               help='dataset of block ID or Rucio DID (scope:name)')


    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.dataset is None:
            msg = "%sError%s: Please specify the dataset to check." % (colors.RED, colors.NORMAL)
            msg += " Use the --dataset option."
            ex = MissingOptionException(msg)
            ex.missingOption = "dataset"
            raise ex
