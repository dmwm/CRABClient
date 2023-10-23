#pylint: disable=consider-using-f-string
"""
allow users to (in)validate some files in their USER datasets in phys03
"""
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException
from CRABClient.ClientUtilities import colors

from CRABClient.CrabRestInterface import HTTPRequests

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
        instance = self.options.instance
        dataset = self.options.dataset
        if instance.startswith('https://'):
            if 'DBSReader' in instance:
                dbsReadUrl = instance
                dbsWriteUrl = instance.replace('DBSReader', 'DBSWriter')
            elif 'DBSWriter' in instance:
                dbsWriteUrl = instance
                dbsReadUrl = instance.replace('DBSWriter', 'DBSReader')
            else:
                msg = 'invalid instance %s' % instance
                raise ConfigurationException(msg)
        else:
            dbsWriteUrl = "https://cmsweb.cern.ch:8443/prod/dbs/phys03/DBSWriter"
            dbsReadUrl = "https://cmsweb.cern.ch:8443/prod/dbs/phys03/DBSReader"

        self.logger.info('instance  = %s' % instance)
        self.logger.info('Read Url  = %s' % dbsReadUrl)
        self.logger.info('Write Url = %s' % dbsWriteUrl)
        self.logger.info('dataset   = %s' % dataset)

        localcert = 'dgfhfeg'
        localkey = 'sfghret'

        dbsReader = HTTPRequests(hostname=dbsReadUrl, localcert=localcert, localkey=localkey,
                                 retry=2, logger=self.logger, verbose=False,
                                 userAgent='CRABClient', version=__version__)

        dbsWriter = HTTPRequests(hostname=dbsWriteUrl, localcert=localcert, localkey=localkey,
                                 retry=2, logger=self.logger, verbose=False,
                                 userAgent='CRABClient', version=__version__)

        ds = '/GenericTTbar/belforte-Stefano-TestRucioP-230817-94ba0e06145abd65ccb1d21786dc7e1d/USER'
        myd = dbsReader.get(uri="dataset",data='dataset=%s'%ds)
        self.logger.info(myd)



        allOK = True

        if allOK:
            return {'commandStatus': 'SUCCESS'}
        else:
            return{'commandStatus': 'FAILED'}


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--instance', dest='instance', default='prod/phys03',
                               help='DBS instance. e.g. prod/phys03 (default) or int/phys03'
                               )
        self.parser.add_option('--dataset', dest='dataset', default=None,
                               help='dataset name')
        self.parser.add_option('--status', dest='status',default=None,
                               help='New status of the dataset: VALID/INVALID/DELETED/DEPRECATED',
                               choices=['VALID', 'INVALID', 'DELETED', 'DEPRECATED']
                               )
        self.parser.add_option('--recursive', dest='recursive', default=False, action="store_true",
                               help='Apply status to children datasets and sets all files status in those' +
                               'as VALID if status=VALID, INVALID otherwise'
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
