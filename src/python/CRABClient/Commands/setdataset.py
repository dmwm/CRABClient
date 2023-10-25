#pylint: disable=consider-using-f-string
"""
allow users to (in)validate their own DBS USER datasets
"""

import sys
import json

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException, CommandFailedException
from CRABClient.ClientUtilities import colors
from CRABClient.CrabRestInterface import HTTPRequests

if sys.version_info >= (3, 0):
    from urllib.parse import urlencode  # pylint: disable=E0611
if sys.version_info < (3, 0):
    from urllib import urlencode

try:
    from CRABClient import __version__
except:  # pylint: disable=bare-except
    __version__ = '0.0.0'


def getDbsREST(instance=None, logger=None, cert=None, key=None, version=None):
    """
    given a DBS istance (e.g. prod/phys03) returns a DBSReader and DBSWriter
    client instances which communicate with DBS REST via curl
    Arguments:
    logger: a logger
    cert, key : name of files, can use the path to X509_USER_PROXY for both
    version: the CRAB Client version to put in the User Agent field of the query
    """
    # if user supplied a simple prod/phys03 like instance, these two lines will do
    # note that our HTTPRequests will add https://
    dbsReadUrl = "cmsweb.cern.ch:8443/dbs/" + instance + "/DBSReader/"
    dbsWriteUrl = "cmsweb.cern.ch:8443/dbs/" + instance + "/DBSWriter/"
    # a possible use case e.g. for testing is to use int instance of DBS. requires testbed CMSWEB
    if instance.startswith('int'):
        dbsReadUrl = dbsReadUrl.replace('cmsweb', 'cmsweb-testbed')
        dbsWriteUrl = dbsWriteUrl.replace('cmsweb', 'cmsweb-testbed')
    # if user knoww better and provided a full URL, we'll take and adapt
    # to have both Reader and Writer,
    if instance.startswith("https://"):
        url = instance.lstrip("https://")  # will be added back in HTTPRequests
        if "DBSReader" in url:
            dbsReadUrl = url
            dbsWriteUrl = url.replace('DBSReader', 'DBSWriter')
        elif 'DBSWriter' in url:
            dbsWriteUrl = url
            dbsReadUrl = url.replace('DBSWriter', 'DBSReader')
        else:
            raise ConfigurationException("bad instance value %s" % instance)

    logger.debug('Read Url  = %s' % dbsReadUrl)
    logger.debug('Write Url = %s' % dbsWriteUrl)

    dbsReader = HTTPRequests(hostname=dbsReadUrl, localcert=cert, localkey=key,
                             retry=2, logger= logger, verbose=False, contentType='application/json',
                             userAgent='CRABClient', version=version)

    dbsWriter = HTTPRequests(hostname=dbsWriteUrl, localcert=cert, localkey=key,
                             retry=2, logger= logger, verbose=False, contentType='application/json',
                             userAgent='CRABClient', version=version)
    return dbsReader, dbsWriter


class setdataset(SubCommand):
    """
    Set status of a USER dataet in phys03,
    optionally invalidates/revalidates all files in it
    meant to replace https://github.com/dmwm/DBS/blob/master/Client/utils/DataOpsScripts/DBS3SetDatasetStatus.py
    and to work whenever CRAB is supported, i.e. with both python2 and python3
    """

    name = 'setdataset'

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)


    def __call__(self):
        result = 'FAILED'  # will change to 'SUCCESS' when all is OK

        instance = self.options.instance
        dataset = self.options.dataset
        status = self.options.status
        recursive = self.options.recursive
        self.logger.debug('instance  = %s' % instance)
        self.logger.debug('dataset   = %s' % dataset)
        self.logger.debug('status    = %s' % status)
        self.logger.debug('recursive = %s' % recursive)

        if recursive:
            self.logger.warning("ATTENTION: recursive option is not implemented yet. Ignoring it")

        # from DBS instance, to DBS REST services
        dbsReader, dbsWriter = getDbsREST(instance=instance, logger=self.logger,
                                          cert=self.proxyfilename, key=self.proxyfilename,
                                          version=__version__)

        self.logger.info("looking up Dataset %s in DBS %s" % (dataset, instance) )
        datasetStatusQuery = {'dataset': dataset, 'dataset_access_type': '*', 'detail': True}
        ds, rc, msg = dbsReader.get(uri="datasets",data=urlencode(datasetStatusQuery))
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

        ds, rc, msg = dbsReader.get(uri="datasets",data=urlencode(datasetStatusQuery))
        self.logger.debug('exitcode= %s', rc)
        self.logger.info("Dataset status in DBS now is %s" % ds[0]['dataset_access_type'])

        return {'commandStatus': result}

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--instance', dest='instance', default='prod/phys03',
                               help="DBS instance. e.g. prod/phys03 (default) or int/phys03. Use at your own risk." +
                                    "Unless you really know what you are doing, stay with the default"
                               )
        self.parser.add_option('--dataset', dest='dataset', default=None,
                               help='dataset name')
        self.parser.add_option('--status', dest='status',default=None,
                               help="New status of the dataset: VALID/INVALID/DELETED/DEPRECATED",
                               choices=['VALID', 'INVALID', 'DELETED', 'DEPRECATED']
                               )
        self.parser.add_option('--recursive', dest='recursive', default=False, action="store_true",
                               help="Apply status to children datasets and sets all files status in those" +
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
        instance = self.options.instance
        if not '/' in instance or len(instance.split('/'))>2 and not instance.startswith('https://'):
            msg = "Bad instance value %s. " % instance
            msg += "Use either server/db format or full URL"
            raise ConfigurationException(msg)
