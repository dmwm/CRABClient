# pylint: disable=consider-using-f-string, unspecified-encoding
"""
allow users to (in)validate some files in their USER datasets in phys03
"""

import json

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException, CommandFailedException
from CRABClient.ClientUtilities import colors
from CRABClient.RestInterfaces import getDbsREST


class setfilestatus(SubCommand):
    """
    Set status of a USER dataset in phys03,
    optionally invalidates/revalidates all files in it
    meant to replace https://github.com/dmwm/DBS/blob/master/Client/utils/DataOpsScripts/DBS3SetDatasetStatus.py
    and to work whenever CRAB is supported, i.e. with both python2 and python3
    """

    name = 'setfilestatus'

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)

    def __call__(self):

        result = 'FAILED'  # will change to 'SUCCESS' when all is OK

        # intitalize, and validate args
        instance = self.options.instance
        dataset = self.options.dataset
        files = self.options.files
        status = self.options.status
        self.logger.debug('instance     = %s' % instance)
        self.logger.debug('dataset      = %s' % dataset)
        self.logger.debug('files        = %s' % files)
        self.logger.debug('status       = %s' % status)

        statusToSet = 1 if status == 'VALID' else 0

        filesToChange = None
        if files:
            # did the user specify the name of a file containing a list of LFN's ?
            try:
                with open(files, 'r') as f:
                    flist = [lfn.strip() for lfn in f]
                    filesToChange = ','.join(flist)
            except IOError:
                # no. Assume we have a comma separated list of LFN's (a single LFN is also OK)
                filesToChange = files.strip(",").strip()
            finally:
                # files and dataset options are mutually exclusive
                dataset = None
            if ',' in filesToChange:
                raise NotImplementedError('list of LFNs is not supported yet')

        # from DBS instance, to DBS REST services
        dbsReader, dbsWriter = getDbsREST(instance=instance, logger=self.logger,
                                          cert=self.proxyfilename, key=self.proxyfilename)
        # we will need the dataset name
        if dataset:
            datasetName = dataset
        else:
            # get it from DBS
            lfn = filesToChange.split(',')[0]
            query = {'logical_file_name': lfn}
            out, rc, msg = dbsReader.get(uri='datasets', data=query)
            if not out:
                self.logger.error("ERROR: file %s not found in DBS" % lfn)
                raise ConfigurationException
            datasetName = out[0]['dataset']
            self.logger.info('LFN to be changed belongs to dataset %s' % datasetName)

        # when acting on a list of LFN's, can't print status of all files before/after
        # best we can do is to print the number of valid/invalid file in the dataset
        # before/after.

        self.logFilesTally(dataset=datasetName, dbs=dbsReader)

        if filesToChange:
            data = {'logical_file_name': filesToChange, 'is_file_valid': statusToSet}
        if dataset:
            data = {'dataset': dataset, 'is_file_valid': statusToSet}
        jdata = json.dumps(data)  # PUT requires data in JSON format
        out, rc, msg = dbsWriter.put(uri='files', data=jdata)
        if rc == 200 and msg == 'OK':
            self.logger.info("File(s) status changed successfully")
            result = 'SUCCESS'
        else:
            msg = "File(s) status change failed: %s" % out
            raise CommandFailedException(msg)

        self.logFilesTally(dataset=datasetName, dbs=dbsReader)

        return {'commandStatus': result}

    def logFilesTally(self, dataset=None, dbs=None):
        """ prints total/valid/invalid files in dataset """
        query = {'dataset': dataset, 'validFileOnly': 1}
        out, _, _ = dbs.get(uri='files', data=query)
        valid = len(out)
        query = {'dataset': dataset, 'validFileOnly': 0}
        out, _, _ = dbs.get(uri='files', data=query)
        total = len(out)
        invalid = total - valid
        self.logger.info("Dataset file count total/valid/invalid = %d/%d/%d" % (total, valid, invalid))

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('-i', '--instance', dest='instance', default='prod/phys03',
                               help='DBS instance. e.g. prod/phys03 (default) or int/phys03'
                               )
        self.parser.add_option('-d', '--dataset', dest='dataset', default=None,
                               help='Will apply status to all files in this dataset.' + \
                                    ' Use either --files or--dataset',
                               metavar='<dataset_name>')
        self.parser.add_option('-s', '--status', dest='status', default=None,
                               help='New status of the file(s): VALID/INVALID',
                               choices=['VALID', 'INVALID']
                               )
        self.parser.add_option('-f', '--files', dest='files', default=None,
                               help='List of files to be validated/invalidated.' + \
                                    ' Can be either a simple LFN or a file containg LFNs or' + \
                                    ' a comma separated list of LFNs. Use either --files or --dataset',
                               metavar="<lfn1[,..,lfnx] or filename>")

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if not self.options.files and not self.options.dataset:
            msg = "%sError%s: Please specify the files to change." % (colors.RED, colors.NORMAL)
            msg += " Use either the --files or the --dataset option."
            ex = MissingOptionException(msg)
            ex.missingOption = "files"
            raise ex
        if self.options.files and self.options.dataset:
            msg = "%sError%s: You can not use both --files and --dataset at same time" % (colors.RED, colors.NORMAL)
            raise ConfigurationException(msg)
        if self.options.status is None:
            msg = "%sError%s: Please specify the new file(s) status." % (colors.RED, colors.NORMAL)
            msg += " Use the --status option."
            ex = MissingOptionException(msg)
            ex.missingOption = "status"
            raise ex
