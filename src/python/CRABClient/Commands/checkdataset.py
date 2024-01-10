"""
check disk availability of a dataset
"""

# avoid complains about things that we can not fix in python2
# pylint: disable=consider-using-f-string, unspecified-encoding, raise-missing-from

from __future__ import print_function, division

import sys
import json

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import execute_command, colors
from CRABClient.ClientUtilities import commandUsedInsideCrab
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException
from CRABClient.RestInterfaces import getDbsREST

if sys.version_info >= (3, 0):
    from urllib.parse import urlencode
if sys.version_info < (3, 0):
    from urllib import urlencode

class checkdataset(SubCommand):
    """
    check availability on disk of a dataset
    """
    name = 'checkdataset'
    shortnames = ['chkd']

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        self.dataset = None
        self.interactive = False
        self.dbsInstance = None

    def __call__(self):

        self.dataset = self.options.dataset
        self.dbsInstance = self.options.dbsInstance
        self.interactive = commandUsedInsideCrab()

        status, size = self.checkDatasetInDBS()
        self.logger.info("dataset status in DBS is: %s", status)
        self.logger.info("dataset size is: %s", size)

        if not self.rucio:
            self.logger.error("Rucio client not available with this CMSSW version. Can't check disk availability")
            return {'commandStatus': 'FAILED'}

        containerScope, containerName, blocks = self.getInputs()
        if not blocks:
            self.logger.info("Dataset is empty or unknown to Rucio")
            return {'commandStatus': 'SUCCESS'}
        blackListedSites = self.getCrabBlacklist()
        if self.interactive:
            print("Checking blocks availabiliyt on disk ...")
        else:
            self.logger.info("Check blocks availabiliyt on disk")
        tapeSites, locationsMap = self.createLocationsMap(blocks)
        self.logger.info("dataset is on tape at: %s", tapeSites)
        (nbFORnr, nbFORrse) = self.createBlockMaps(locationsMap=locationsMap, blackList=[])
        self.printBlocksPerReplicaMap(nbFORnr)

        self.logger.info("\n Block locations:")
        for rse in nbFORrse.keys():
            msg = " %20s hosts %3s blocks" % (rse, nbFORrse[rse])
            if rse in blackListedSites:
                msg += "  *SITE BLACKLISTED IN CRAB*"
            self.logger.info(msg)
        self.logger.info("")

        self.logger.info("AFTER APPLYING CRAB SITE BLACKLIST:")
        (nbFORnr, nbFORrse) = self.createBlockMaps(locationsMap=locationsMap, blackList=blackListedSites)
        self.printBlocksPerReplicaMap(nbFORnr)

        self.logger.info("\nRules on this dataset:")
        ruleGens = self.rucio.list_did_rules(scope=containerScope, name=containerName)
        rules = list(ruleGens)
        if rules:
            pattern = '{:^32}{:^20}{:^10}{:^20}{:^30}'
            self.logger.info(pattern.format('ID', 'account', 'state', 'expiration', 'RSEs'))
            for r in rules:
                self.logger.info(pattern.format(r['id'], r['account'], r['state'], str(r['expires_at']), r['rse_expression']))
        else:
            self.logger.info("NONE")

        return {'commandStatus': 'SUCCESS'}

    def checkDatasetInDBS(self):
        """ return status and size (including unit) of the dataset as strings """

        status = 'NotFound'
        size = 0

        dbsReader, _ = getDbsREST(instance=self.dbsInstance, logger=self.logger,
                                  cert=self.proxyfilename, key=self.proxyfilename)
        datasetName = self.dataset.split(':')[-1]
        isBlock = datasetName if  '#' in datasetName else None
        if not isBlock:
            query = {'dataset': datasetName, 'dataset_access_type': '*', 'detail': True}
            ds, rc, msg = dbsReader.get(uri="datasets", data=urlencode(query))
            if not ds:
                self.logger.error("ERROR: %s not found in DBS", datasetName)
                return status, size
        else:
            query =  {'block_name': datasetName}
            bs, rc, msg = dbsReader.get(uri="blocks", data=urlencode(query))
            if not bs:
                self.logger.error("ERROR: %s not found in DBS", datasetName)
                return status, size
        self.logger.debug('exitcode= %s', rc)
        if rc != 200:  # this is HTTP code. 200=OK
            self.logger.error("Error trying to talk with DBS:\n%s", msg)
            return status, size
        # OK, dataset/block found in DBS
        if isBlock:
            status = 'EXISTS'  # only datasets and files have status in DBS
            query = {'block_name': datasetName}
        else:
            status = ds[0]['dataset_access_type']
            query = {'dataset': datasetName}
        fs, rc, msg = dbsReader.get(uri='filesummaries', data=urlencode(query))
        byteSize = fs[0]['file_size']
        gBytes = byteSize / 1000. / 1000. / 1000.
        if gBytes > 10000:
            size = "%1.f TB" % (gBytes / 1000.)
        else:
            size = "%s GB" % int(gBytes)

        return status, size


    def getInputs(self):
        """
        get name of object to test and check what it is
        returns:
        containerScope, containerName (the DID)
        blocks = [b1, b2...] a list of block names in the dataset
        """

        if ':' in self.dataset:
            scope = self.dataset.split(':')[0]
            name = self.dataset.split(':')[1]
        else:
            scope = 'cms'
            name = self.dataset
        if scope == 'cms':
            self.logger.info("Checking disk availability of dataset: %s", name)
        else:
            self.logger.info("Checking disk availability of container: %s", name)
        if scope != 'cms':
            self.logger.info(" container in USER scope. Assume it contains datasets(blocks) in CMS scope")
        self.logger.info(" only fully available (i.e. complete) block replicas are considered ")
        containerScope = scope
        containerName = name

        # get list of blocks (datasets in Rucio)
        if '#' in name:
            self.logger.info("Input is a DBS-block (Rucio-dataset) will check that one")
            blocks = [name]
        else:
            from rucio.common.exception import DataIdentifierNotFound
            try:
                dss = self.rucio.list_content(scope=scope, name=name)
                blocks = [ds['name'] for ds in dss]
            except DataIdentifierNotFound:
                self.logger.error('Dataset not found in Rucio')
                blocks = []
            self.logger.info("dataset has %d blocks", len(blocks))

        return containerScope, containerName, blocks

    def createBlockMaps(self, locationsMap=None, blackList=None):
        """
        creates 2 maps: nReplicas-->nBlocks and rse-->nBlocks
        locationsMap is a dictionary {block:[replica1, replica2..]}
        """
        nbFORnr = {}
        nbFORnr[0] = 0  # this will not be filled in the loop if every block has locations
        nbFORrse = {}
        for block in locationsMap:
            replicas = locationsMap[block]
            if replicas:
                for rse in replicas.copy():
                    if rse in blackList:
                        replicas.remove(rse)
                try:
                    nbFORnr[len(replicas)] += 1
                except KeyError:
                    nbFORnr[len(replicas)] = 1
                for rse in locationsMap[block]:
                    try:
                        nbFORrse[rse] += 1
                    except KeyError:
                        nbFORrse[rse] = 1
            else:
                nbFORnr[0] += 1
        return (nbFORnr, nbFORrse)

    def getCrabBlacklist(self):
        """  let's make a list of sites where CRAB will not run """

        usableSitesUrl = 'https://cmssst.web.cern.ch/cmssst/analysis/usableSites.json'
        cmd = "curl -s %s" % usableSitesUrl
        out, err, ec = execute_command(cmd)
        if not ec:
            usableSites = json.loads(out)
        else:
            self.logger.error("EROR retrieving list of blacklisted sites:\n%s", err)
            self.logger.info("Will not use a blacklist")
            usableSites = []
        #result = subprocess.run(f"curl -s {usableSitesUrl}", shell=True, stdout=subprocess.PIPE)
        #usableSites = json.loads(result.stdout.decode('utf-8'))
        blackListedSites = []
        for site in usableSites:
            if 'value' in site and site['value'] == 'not_usable':
                blackListedSites.append(site['name'])
        return blackListedSites

    def createLocationsMap(self, blocks):
        """
        following loop is copied from CRAB DBSDataDiscovery
        locationsMap is a dictionary: key=blockName, value=list of RSEs}
        nbFORnr is dictionary: key= number of RSEs with a block, value=number of blocks with that # or RSEs
        nbFORrse is a dictionary: key=RSEname, value=number of blocks available at that RSE
        this should be rewritten so that the two dicionaries are filled via a separate loop on
        locationsMap content. Makes it easier to read, debug, improve
        """
        locationsMap = {}
        tapeSites = set()
        nb = 0
        for blockName in blocks:
            nb += 1
            if self.interactive:
                print("  block: %d" % nb, end='\r')
            replicas = set()
            response = self.rucio.list_dataset_replicas(scope='cms', name=blockName, deep=True)
            for item in response:
                if 'Tape' in item['rse']:
                    tapeSites.add(item['rse'])
                    continue  # skip tape locations
                if 'T3_CH_CERN_OpenData' in item['rse']:
                    continue  # ignore OpenData until it is accessible by CRAB
                if item['state'].upper() == 'AVAILABLE':  # means all files in the block are on disk
                    replicas.add(item['rse'])
            locationsMap[blockName] = replicas
        if self.interactive:
            print("")
        return tapeSites, locationsMap

    def printBlocksPerReplicaMap(self, nbFORnr):
        """
        print how many blocks have 0, 1, 2... disk replicas
        """
        nBlocks = 0
        for nr in sorted(list(nbFORnr.keys())):
            nBlocks += nbFORnr[nr]
            msg = " %3s blocks have %2s disk replicas" % (nbFORnr[nr], nr)
            if nr == 0 and nbFORnr[0] > 0:
                msg += " *THESE BLOCKS WILL NOT BE ACCESSIBLE*"
            self.logger.info(msg)
        if not nbFORnr[0]:
            self.logger.info(" Dataset is fully available")
        else:
            nAvail = nBlocks - nbFORnr[0]
            self.logger.info(" Only %d/%d are available", nAvail, nBlocks)
        return

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option('--dataset',
                               dest='dataset',
                               default=None,
                               help='dataset of block ID or Rucio DID (scope:name)')
        self.parser.add_option('--dbs-instance', dest='dbsInstance', default='prod/global',
                               help="DBS instance. e.g. prod/global (default) or prod/phys03 or full URL."
                                    + "\nUse at your own risk only if you really know what you are doing"
                               )

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.dataset is None:
            msg = ("%sError%s: Please specify the dataset to check."  # pylint: disable=consider-using-f-string
                   % (colors.RED, colors.NORMAL))
            msg += " Use the --dataset option."
            ex = MissingOptionException(msg)
            ex.missingOption = "dataset"
            raise ex
        dbsInstance = self.options.dbsInstance
        if '/' not in dbsInstance or len(dbsInstance.split('/')) > 2 and not dbsInstance.startswith('https://'):
            msg = "Bad DBS instance value %s. " % dbsInstance  # pylint: disable=consider-using-f-string
            msg += "Use either server/db format or full URL"
            raise ConfigurationException(msg)
