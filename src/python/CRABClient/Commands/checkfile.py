"""
check a file (LFN) status in DBS and Rucio and its disk replicas
"""

# avoid complains about things that we can not fix in python2
# pylint: disable=consider-using-f-string, unspecified-encoding, raise-missing-from
from __future__ import print_function

import sys
import tempfile

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.UserUtilities import getUsername
from CRABClient.ClientUtilities import execute_command, colors
from CRABClient.ClientUtilities import commandUsedInsideCrab
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException

from CRABClient.RestInterfaces import getDbsREST


if sys.version_info >= (3, 0):
    from urllib.parse import urlencode
if sys.version_info < (3, 0):
    from urllib import urlencode


class checkfile(SubCommand):
    """
    check a file (LFN):
    is it valid in DBS ? in Rucio ? is it on disk ? are replicas OK ?
    """
    name = 'checkfile'

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        self.fileToCheck = {}
        self.dbsInstance = None
        self.checkChecksum = False
        self.lfn = None

    def __call__(self):

        self.dbsInstance = self.options.dbsInstance
        self.lfn = self.options.lfn
        self.fileToCheck['scope'] = self.options.scope
        self.checkChecksum = self.options.checkChecksum

        if self.checkChecksum:
            msg = "Beware, several GB of disk space on /tmp are needed for checking replica cheksum(s)"
            msg += "\nIf you do not have them, create the TMPDIR enviromental variable"
            msg += "\nand point it to an existing directory of your choice\n"
            self.logger.info(msg)

        self.logger.info("looking up LFN in DBS %s", self.dbsInstance)
        found, validInDBS = self.checkFileInDBS()
        if not found:
            self.logger.error('LFN not found in DBS')
            return {'commandStatus': 'SUCCESS'}

        if not self.rucio:
            self.logger.warning('Rucio client not available with CMSSW<10\n No more checks possible')
            return {'commandStatus': 'SUCCESS'}

        self.logger.info("Check information in Rucio")
        status, msg = self.checkFileInRucio()
        if not status:
            self.logger.error('LFN not found or otherwise not properly stored in Rucio')
            self.logger.error('Details: %s', msg)
            if validInDBS:
                self.logger.error("ERROR: most likely file was deleted but non invalidated in DBS")
            else:
                self.logger.info("This is consistente with INVALID in DBS")
            return {'commandStatus': 'SUCCESS'}

        # so far so good, find Replicas and check size of the disk ones

        nDisk, diskReplicas = self.findDiskReplicas()
        self.logger.info("LFN has %s disk replicas", nDisk)
        if nDisk == 0:
            return {'commandStatus': 'SUCCESS'}
        msg = "List of disk replicas. Check that file exists and has correct size "
        msg += "(%s bytes):" % self.fileToCheck['size']
        self.logger.info(msg)

        self.logger.info("%15s    status", 'RSE')
        rseWithSizeOK = []
        for rse, pfn in diskReplicas.items():
            isSizeOK, msg = self.checkReplicaSize(pfn)
            if isSizeOK:
                self.logger.info("%15s      OK", rse)
                rseWithSizeOK.append(rse)
            else:
                self.logger.error("%15s  something wrong", rse)
                self.logger.error(msg)

        if rseWithSizeOK and not self.checkChecksum:
            msg = "Disk replicas files may still be corrupted"
            msg += "\n run again with --checksum for a thorough check. Beware: SLOW and needs GB's of disk"
            self.logger.info(msg)
            return {'commandStatus': 'SUCCESS'}

        # reach here if user asked to check replica's checksum
        if rseWithSizeOK:
            self.logger.info("\nCheck Adler32 checksum (%s) for each disk replica", self.fileToCheck['adler32'])
        for rse in rseWithSizeOK:
            pfn = diskReplicas[rse]
            if commandUsedInsideCrab():
                print("verify checksum for replica at %s. " % rse, end="", flush=True)

            isAdlerOK, msg = self.checkReplicaAdler32(pfn)
            if isAdlerOK:
                self.logger.info("%15s      OK", rse)
            else:
                self.logger.error("%15s  something wrong", rse)
                self.logger.error(msg)

        return {'commandStatus': 'SUCCESS'}

    def checkFileInDBS(self):
        """ check that LFN is present and VALID in DBS. Add to self block and dataset it belongs to """
        dbsReader, _ = getDbsREST(instance=self.dbsInstance, logger=self.logger,
                                  cert=self.proxyfilename, key=self.proxyfilename)
        query = {'logical_file_name': self.lfn, 'validFileOnly': False, 'detail': True}
        fs, rc, msg = dbsReader.get(uri="files", data=urlencode(query))
        self.logger.debug('exitcode= %s', rc)
        if rc != 200:  # this is HTTP code. 200=OK
            self.logger.error("Error trying to talk with DBS:\n%s", msg)
            return False, False
        if not fs:
            self.logger.error("ERROR: LFN %s not found in DBS", self.lfn)
            return False, False
        fileStatus = 'VALID' if fs[0]['is_file_valid'] else 'INVALID'
        self.logger.info("  file status in DBS is %s", fileStatus)
        dbsDataset = fs[0]['dataset']
        block = fs[0]['block_name']
        query = {'dataset' : dbsDataset, 'dataset_access_type': '*', 'detail': True}
        ds, rc, msg = dbsReader.get(uri="datasets", data=urlencode(query))
        self.logger.debug('exitcode= %s', rc)
        if rc != 200:  # this is HTTP code. 200=OK
            self.logger.info("Error trying to talk with DBS:\n%s", msg)
        if not ds:
            self.logger.error("ERROR: DATASET %s not found in DBS", dbsDataset)
            return False, fileStatus == 'VALID'
        datasetStatus = ds[0]['dataset_access_type']
        self.logger.info("  file belongs to:\n    dataset: %s", dbsDataset)
        self.logger.info("    block:   %s", block)
        msg = "    dataset is :   %s" % datasetStatus
        if datasetStatus != 'VALID':
            msg += "  - **WARNING** this file may not exist ot be otherwise unusable"
        self.logger.info(msg)
        self.fileToCheck['block'] = block
        self.fileToCheck['dataset'] = dbsDataset

        return True, fileStatus == 'VALID'

    def checkFileInRucio(self):
        """
        check that file is present in Rucio and has same parentage as in DBS
        Add to self expected size and adler32 of file replicas
        """
        from rucio.common.exception import DataIdentifierNotFound  # pylint: disable=import-outside-toplevel

        scope = self.fileToCheck['scope']
        if not scope:
            if 'global' in self.dbsInstance:
                scope = 'cms'
            else:
                username = getUsername(self.proxyfilename, logger=self.logger)
                scope = 'user:' + username
            self.fileToCheck['scope'] = scope

        # check if file is present and attached to a container/dataset
        try:
            did = self.rucio.get_did(scope=scope, name=self.lfn)
            lfnSize = did['bytes']
            lfnAdler32 = did['adler32']
        except DataIdentifierNotFound:
            msg = "LFN not found in Rucio"
            return False, msg

        self.logger.debug('LFN found in Rucio')
        dids = list(self.rucio.list_parent_dids(scope=scope, name=self.lfn))
        if not dids:
            msg = "file does not belong to any Rucio dataset"
            msg += "\nERROR: most likely file was deleted/lost before stored to tape and not invalidated in DBS"
            return False, msg
        rucioDataset = dids[0]['name']
        self.logger.debug('LFN belongs to RucioDataset: %s', rucioDataset)
        dids = list(self.rucio.list_parent_dids(scope=scope, name=rucioDataset))
        if not dids:
            msg = "Rucio Dataset does not belong to any Rucio container"
            return False, msg
        rucioContainer = dids[0]['name']
        self.logger.debug('LFN belongs to RucioContainer: %s', rucioContainer)

        if rucioDataset != self.fileToCheck['block'] or rucioContainer != self.fileToCheck['dataset']:
            self.logger.error('Rucio/DBS mismatch')
            msg = "Ruciod dataset/container do not match DBS block/dataset"
            msg += "\n Rucio container: %s" % rucioContainer
            msg += "\n Rucio dataset  : %s" % rucioDataset
            return False, msg

        self.logger.info('  LFN found in Rucio with matching block/dataset parentage')
        self.fileToCheck['size'] = lfnSize
        self.fileToCheck['adler32'] = lfnAdler32
        return True, ""

    def getReplicaList(self):
        """ creates a dictionary {RSE:pfn, RSE:pfn...}  """
        replicaList = {}
        replicaGen = self.rucio.list_replicas([{'scope': self.fileToCheck['scope'],
                                                'name': self.lfn}])
        replica = next(replicaGen)  # since we passed a lit of one DID as arg, there's only on replica object
        for rse, pfn in replica['rses'].items():
            replicaList[rse] = pfn[0]
        return replicaList

    def findDiskReplicas(self):
        """ returns a dictionary {rse: pfn, rse: pfn,...} """
        replicas = self.getReplicaList()
        diskReplicas = {}
        nDisk = 0
        for rse, pfn in replicas.items():
            if 'Tape' in rse:
                self.logger.info("LFN has a tape replica at %s", rse)
            else:
                nDisk += 1
                diskReplicas[rse] = pfn
        return nDisk, diskReplicas

    def checkReplicaSize(self, pfn):
        """ verify size of a remote file replica (pfn) """
        cmd = "eval `scram unsetenv -sh`; gfal-ls -l %s" % pfn
        out, err, ec = execute_command(cmd)
        if ec:
            if 'File not found' in err or 'No such file' in err:
                msg = "File not found"
                return False, msg
        try:
            replicaSize = int(out.split()[4])
        except Exception:  # pylint: disable=broad-except
            msg = "Error looking up file at remote site"
            return False, msg
        if replicaSize != self.fileToCheck['size']:
            msg = "Replica size at remote side is wrong. %s vs. %s" % (replicaSize, self.fileToCheck['size'])
            msg += "  Needs to be invalidated"
            return False, msg
        return True, ""

    def checkReplicaAdler32(self, pfn):
        """
        compute Adler32 on a remote PFN. Need to copy the file first
        and run gfal-sum on local copy. gfal-sum accepts a remote PFN
        but in that case it returns some value stored by remote server
        which can be stale (the checksum when the file was written ?)
        """

        f = tempfile.NamedTemporaryFile(delete=False, prefix='testFile_')
        fname = f.name
        execute_command("rm -f %s" % fname)
        if commandUsedInsideCrab():
            print("copy file...", end="", flush=True)
        cmd = "eval `scram unsetenv -sh`; gfal-copy %s %s" % (pfn, fname)
        out, err, ec = execute_command(cmd)
        if ec:
            msg = "file copy for checksum control failed.\n%s\n%s" % (out, err)
            execute_command("rm -f %s" % fname)
            return False, msg

        if commandUsedInsideCrab():
            print("compute Adler32 checksum...", end="", flush=True)
        cmd = "eval `scram unsetenv -sh`; gfal-sum %s ADLER32" % fname
        out, err, ec = execute_command(cmd)
        if ec:
            msg = "checksum computation failed.\n%s\n%s" % (out, err)
            execute_command("rm -f %s" % fname)
            return False, msg
        if commandUsedInsideCrab():
            print("Done")
        adler32 = out.split()[1]
        if not adler32 == self.fileToCheck['adler32']:
            msg = "Remote replica has wrong checksum. %s vs. %s" % (adler32, self.fileToCheck['adler32'])
            msg += "  Needs to be invalidated"
            execute_command("rm -f %s" % fname)
            return False, msg
        execute_command("rm -f %s" % fname)
        return True, ""

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--lfn',
                               dest='lfn',
                               default=None,
                               help='LFN of the file to check')
        self.parser.add_option('--checksum',
                               dest='checkChecksum',
                               action="store_true",
                               help="check checksum of all disk replicas. SLOW and needs GB's of disk !")
        self.parser.add_option('--dbs-instance', dest='dbsInstance', default='prod/global',
                               help="DBS instance. e.g. prod/global (default) or prod/phys03 or full URL."
                                    + "\nUse at your own risk only if you really know what you are doing"
                               )
        self.parser.add_option('--rucio-scope', dest='scope', default=None,
                               help="Rucio scope. Default is 'cms' for global DBS and 'user:username' for phys03"
                               )

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.lfn is None:
            msg = "%sError%s: Please specify the LFN to check." % (colors.RED, colors.NORMAL)  # pylint: disable=consider-using-f-string
            msg += " Use the --lfn option."
            ex = MissingOptionException(msg)
            ex.missingOption = "lfn"
            raise ex

        dbsInstance = self.options.dbsInstance
        if '/' not in dbsInstance or len(dbsInstance.split('/')) > 2 and not dbsInstance.startswith('https://'):
            msg = "Bad DBS instance value %s. " % dbsInstance  # pylint: disable=consider-using-f-string
            msg += "Use either server/db format or full URL"
            raise ConfigurationException(msg)
