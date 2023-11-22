import sys
import tempfile

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.UserUtilities import getUsername
from CRABClient.ClientUtilities import execute_command, colors
from CRABClient.ClientExceptions import MissingOptionException

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
    shortnames = ['chkd']

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        self.fileToCheck = {}
        self.instance = None
        self.checkChecksum = False
        self.lfn = None


    def __call__(self):


        self.instance = self.options.instance
        self.lfn = self.options.lfn
        self.fileToCheck['scope'] = self.options.scope
        self.checkChecksum = self.options.checkChecksum

        self.logger.info("looking up LFN in DBS %s", self.instance)
        found = self.checkFileInDBS()
        if not found:
            self.logger.error('LFN not found in DBS')
            return {'commandStatus': 'FAILED'}

        if not self.rucio:
            self.logger.warning('Rucio client not available with CMSSW<10\n No more checks possible')
            return {'commandStatus': 'SUCCESS'}
        # following code is only executed in python3, can use f-string etc.
        self.logger.info("Check information in Rucio")
        status, msg  = self.checkFileInRucio()
        if not status:
            self.logger.error('LFN not found in Rucio or otherwise not properly stored in Rucio')
            self.logger.error('Details: %s', msg)
            return {'commandStatus': 'FAILED'}

        # so far so good, find Replicas and check size of the disk ones

        nDisk, diskReplicas = self.findDiskReplicas()
        self.logger.info("LFN has %s disk replicas", nDisk)
        if nDisk == 0:
            return {'commandStatus': 'SUCCESS'}
        self.logger.info("List of disk replicas. Check that file exists and has correct size:")
        self.logger.info(f"{'RSE':^15s}    status")
        rseWithSizeOK = []
        for  rse, pfn in diskReplicas.items():
            isSizeOK, msg = self.checkReplicaSize(pfn)
            if isSizeOK:
                self.logger.info(f"{rse:<15s}      OK")
                rseWithSizeOK.append(rse)
            else:
                self.logger.error(f"{rse:<15s}  something wrong")
                self.logger.error(msg)

        if rseWithSizeOK and not self.checkChecksum:
            msg = "Disk replicas files may still be corrupted"
            msg += "\n run again with --checksum for a thorough check. Beware: SLOW"
            self.logger.info(msg)
            return {'commandStatus': 'SUCCESS'}

        if rseWithSizeOK:
            self.logger.info("\nCheck of Adler32 checksum for each disk replica")
        for rse in rseWithSizeOK:
            pfn = diskReplicas[rse]
            print(f"verify checksum for replica at {rse}. ", end="", flush=True)
            isAdlerOK, msg = self.checkReplicaAdler32(pfn)
            if isAdlerOK:
                self.logger.info(f"{rse:<15s}      OK")
            else:
                self.logger.error(f"{rse:<15s}  something wrong")
                self.logger.error(msg)

        return {'commandStatus': 'SUCCESS'}

    def checkFileInDBS(self):
        dbsReader, _ = getDbsREST(instance=self.instance, logger=self.logger,
                                  cert=self.proxyfilename, key=self.proxyfilename)
        query = {'logical_file_name': self.lfn, 'validFileOnly': False, 'detail': True}
        fs, rc, msg = dbsReader.get(uri="files", data=urlencode(query))
        self.logger.debug('exitcode= %s', rc)
        if rc:
            self.logger.error("Error trying to talk with DBS:\n%s" % msg)
            return False
        if not fs:
            self.logger.error("ERROR: LFN %s not found in DBS" % self.lfn)
            return False
        fileStatus = 'VALID' if fs[0]['is_file_valid'] else 'INVALID'
        self.logger.info("  file status in DBS is %s" % fileStatus)
        dbsDataset = fs[0]['dataset']
        block = fs[0]['block_name']
        self.logger.info("  file belongs to:\n    dataset: %s" % dbsDataset)
        self.logger.info("    block:   %s" % block)
        self.fileToCheck['block'] = block
        self.fileToCheck['dataset'] = dbsDataset

        return True

    def checkFileInRucio(self):
        from rucio.common.exception  import DataIdentifierNotFound

        scope = self.fileToCheck['scope']
        if not scope:
            if 'global' in self.instance:
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
            msg += "\nERROR: most likely file was deleted but non invalidated in DBS"
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
            msg += f"\n Rucio container: {rucioContainer}"
            msg += f"\n Rucio dataset  : {rucioDataset}"
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
        cmd = f"eval `scram unsetenv -sh`; gfal-ls -l {pfn}"
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
            msg = f"Replica size at remote side is wrong. {replicaSize} vs. {self.fileToCheck['size']}"
            msg += "  Needs to be invalidated"
            return False, msg
        return True, ""

    def checkReplicaAdler32(self, pfn):
        """
        compute Adler32 on a remote PFN
        """

        f = tempfile.NamedTemporaryFile(delete=False, prefix='testFile_')
        fname = f.name
        execute_command(f"rm -f {fname}")
        print("copy file...", end="", flush=True)
        cmd = f"eval `scram unsetenv -sh`; gfal-copy {pfn} {fname}"
        out, err, ec = execute_command(cmd)
        if ec:
            msg = f"file copy for checksum control failed.\n{out}\n{err}"
            execute_command(f"rm -f {fname}")
            return False, msg
        print("compute Adler32 checksum...", end="", flush=True)
        cmd = f"eval `scram unsetenv -sh`; gfal-sum {fname} ADLER32"
        out, err, ec = execute_command(cmd)
        if ec:
            msg = f"checksum computation failed.\n{out}\n{err}"
            execute_command(f"rm -f {fname}")
            return False, msg
        print("Done")
        adler32 = out.split()[1]
        if not adler32 == self.fileToCheck['adler32']:
            msg = f"Remote replica has wrong checksum. {adler32} vs. {self.fileToCheck['adler32']}"
            msg += "  Needs to be invalidated"
            execute_command(f"rm -f {fname}")
            return False, msg
        execute_command(f"rm -f {fname}")
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
                               help='check checksum of all disk replicas. SLOW !')
        self.parser.add_option('--instance', dest='instance', default='prod/global',
                               help="DBS instance. e.g. prod/global (default) or prod/phys03"
                               )
        self.parser.add_option('--scope', dest='scope', default=None,
                               help="Rucio scope. Default is 'cms' for global DBS and 'user:username' for Phys03"
                               )

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.lfn is None:
            msg = "%sError%s: Please specify the LFN to check." % (colors.RED, colors.NORMAL)
            msg += " Use the --lfn option."
            ex = MissingOptionException(msg)
            ex.missingOption = "lfn"
            raise ex
