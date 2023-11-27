# pylint: disable=consider-using-f-string, unspecified-encoding
"""
allow users to (in)validate their own DBS USER datasets
"""

import sys
import json

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException, CommandFailedException
from CRABClient.ClientUtilities import colors
from CRABClient.RestInterfaces import getDbsREST

if sys.version_info >= (3, 0):
    from urllib.parse import urlencode  # pylint: disable=E0611
if sys.version_info < (3, 0):
    from urllib import urlencode


class setdatasetstatus(SubCommand):
    """
    Set status of a USER dataset in phys03
    """

    name = 'setdatasetstatus'

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)

    def __call__(self):
        result = 'FAILED'  # will change to 'SUCCESS' when all is OK

        dbsInstance = self.options.dbsInstance
        dataset = self.options.dataset
        status = self.options.status
        recursive = self.options.recursive
        self.logger.debug('dbsInstance  = %s' % dbsInstance)
        self.logger.debug('dataset      = %s' % dataset)
        self.logger.debug('status       = %s' % status)
        self.logger.debug('recursive    = %s' % recursive)

        if recursive:
            self.logger.warning("ATTENTION: recursive option is not implemented yet. Ignoring it")

        # from DBS instance, to DBS REST services
        dbsReader, dbsWriter = getDbsREST(instance=dbsInstance, logger=self.logger,
                                          cert=self.proxyfilename, key=self.proxyfilename)

        self.logger.info("looking up Dataset %s in DBS %s" % (dataset, dbsInstance))
        datasetStatusQuery = {'dataset': dataset, 'dataset_access_type': '*', 'detail': True}
        ds, rc, msg = dbsReader.get(uri="datasets", data=urlencode(datasetStatusQuery))
        self.logger.debug('exitcode= %s', rc)
        if not ds:
            self.logger.error("ERROR: dataset %s not found in DBS" % dataset)
            raise ConfigurationException
        self.logger.info("Dataset status in DBS is %s" % ds[0]['dataset_access_type'])
        self.logger.info("Will set it to %s" % status)
        data = {'dataset': dataset, 'dataset_access_type': status}
        jdata = json.dumps(data)
        out, rc, msg = dbsWriter.put(uri='datasets', data=jdata)
        if rc == 200 and msg == 'OK':
            self.logger.info("Dataset status changed successfully")
            result = 'SUCCESS'
        else:
            msg = "Dataset status change failed: %s" % out
            raise CommandFailedException(msg)

        ds, rc, msg = dbsReader.get(uri="datasets", data=urlencode(datasetStatusQuery))
        self.logger.debug('exitcode= %s', rc)
        self.logger.info("Dataset status in DBS now is %s" % ds[0]['dataset_access_type'])

        self.logger.info("NOTE: status of files inside the dataset has NOT been changed")

        return {'commandStatus': result}

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--dbs-instance', dest='dbsInstance', default='prod/phys03',
                               help="DBS instance. e.g. prod/phys03 (default) or int/phys03 or full URL." + \
                                    "\nUse at your own risk only if you really know what you are doing"
                               )
        self.parser.add_option('--dataset', dest='dataset', default=None,
                               help='dataset name')
        self.parser.add_option('--status', dest='status', default=None,
                               help="New status of the dataset: VALID/INVALID/DELETED/DEPRECATED",
                               choices=['VALID', 'INVALID', 'DELETED', 'DEPRECATED']
                               )
        self.parser.add_option('--recursive', dest='recursive', default=False, action="store_true",
                               help="Apply status to children datasets and sets all files status in those" + \
                               "to VALID if status=VALID, INVALID otherwise"
                               )

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.dataset is None:
            msg = "%sError%s: Please specify the dataset to check." % (colors.RED, colors.NORMAL)
            msg += " Use the --dataset option."
            ex = MissingOptionException(msg)
            ex.missingOption = "dataset"
            raise ex
        if self.options.status is None:
            msg = "%sError%s: Please specify the new dataset status." % (colors.RED, colors.NORMAL)
            msg += " Use the --status option."
            ex = MissingOptionException(msg)
            ex.missingOption = "status"
            raise ex
            # minimal sanity check
        dbsInstance = self.options.dbsInstance
        if not '/' in dbsInstance or len(dbsInstance.split('/'))>2 and not dbsInstance.startswith('https://'):
            msg = "Bad DBS instance value %s. " % dbsInstance
            msg += "Use either server/db format or full URL"
            raise ConfigurationException(msg)
