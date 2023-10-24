#pylint: disable=consider-using-f-string
"""
allow users to (in)validate some files in their USER datasets in phys03
"""

import sys
import json

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException, CommandFailedException
from CRABClient.ClientUtilities import colors
from CRABClient.Commands.setdataset import getDbsREST

if sys.version_info >= (3, 0):
    from urllib.parse import urlencode  # pylint: disable=E0611
if sys.version_info < (3, 0):
    from urllib import urlencode

try:
    from CRABClient import __version__
except:  # pylint: disable=bare-except
    __version__ = '0.0.0'

class setfiles(SubCommand):
    """
    Set status of a USER dataset in phys03,
    optionally invalidates/revalidates all files in it
    meant to replace https://github.com/dmwm/DBS/blob/master/Client/utils/DataOpsScripts/DBS3SetDatasetStatus.py
    and to work whenever CRAB is supported, i.e. with both python2 and python3
    """

    name = 'setfiles'

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        #self.instance = None
        #self.dataset = None
        #self.status = None
        #self.recursive = None


    def __call__(self):

        result = 'FAILED'  # will change to 'SUCCESS' when all is OK

        instance = self.options.instance
        dataset = self.options.dataset
        block = self.options.block
        lfn = self.options.lfn
        #lfnList = self.options.lfnList
        #fileWithList = self.options.fileWithList
        status = self.options.status
        self.logger.debug('instance     = %s' % instance)
        self.logger.debug('dataset      = %s' % dataset)
        self.logger.debug('block        = %s' % block)
        self.logger.debug('lfn          = %s' % lfn)
        #self.logger.debug('lfnList      = %s' % lfnList)
        #self.logger.debug('fileWithList = %s' % fileWithList)
        self.logger.debug('status       = %s' % status)

        # from DBS instance, to DBS REST services
        dbsReader, dbsWriter = getDbsREST(instance=instance, logger=self.logger,
                                          cert=self.proxyfilename, key=self.proxyfilename,
                                          version=__version__)


        self.logger.info("looking up LFN %s in DBS %s" % (lfn, instance) )
        lfnStatusQuery = {'logical_file_name': lfn, 'detail': True}
        out, rc, msg = dbsReader.get(uri="files",data=lfnStatusQuery)
        self.logger.debug('exitcode= %s', rc)
        if not out:
            self.logger.error("ERROR: LFN %s not found in DBS" % lfn)
            raise ConfigurationException
        statusInDB = 'VALID' if out[0]['is_file_valid'] == 1 else 'INVALID'
        self.logger.info("File status in DBS is %s" % statusInDB)
        self.logger.info("Will set it to %s" % status)


        statusToSet = 1 if status == 'VALID' else 0
        data = {'logical_file_name': lfn, 'is_file_valid': statusToSet}
        jdata = json.dumps(data)
        out, rc, msg = dbsWriter.put(uri='files', data=jdata)
        if rc == 200 and msg == 'OK':
            self.logger.info("Dataset status changed successfully")
            result = 'SUCCESS'
        else:
            msg = "Dataset status change failed: %s" % out
            raise CommandFailedException(msg)

        out, rc, msg = dbsReader.get(uri="files",data=urlencode(lfnStatusQuery))
        self.logger.debug('exitcode= %s', rc)
        statusInDB = 'VALID' if out[0]['is_file_valid'] == 1 else 'INVALID'
        self.logger.info("LFN status in DBS now is %s" % statusInDB)

        return {'commandStatus': result}


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--instance', dest='instance', default='prod/phys03',
                               help='DBS instance. e.g. prod/phys03 (default) or int/phys03'
                               )
        self.parser.add_option('--dataset', dest='dataset', default=None,
                               help='Will apply status to all files in this dataset')
        self.parser.add_option('--block', dest='block', default=None,
                               help='Will apply status to all files in this block')
        self.parser.add_option('--status', dest='status',default=None,
                               help='New status of the file(s): VALID/INVALID',
                               choices=['VALID', 'INVALID']
                               )
        self.parser.add_option('--lfn', dest='lfn', default=None,
                               help='LFN to change status of')


    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.lfn is None:
            msg = "%sError%s: Please specify the dataset to check." % (colors.RED, colors.NORMAL)
            msg += " Use the --lfn option."
            ex = MissingOptionException(msg)
            ex.missingOption = "lfn"
            raise ex
        if self.options.status is None:
            msg = "%sError%s: Please specify the new file(s) status." % (colors.RED, colors.NORMAL)
            msg += " Use the --status option."
            ex = MissingOptionException(msg)
            ex.missingOption = "status"
            raise ex
