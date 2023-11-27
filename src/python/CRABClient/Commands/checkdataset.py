"""
check disk availability of a dataset
"""
import json

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import execute_command, colors
from CRABClient.ClientUtilities import commandUsedInsideCrab
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
        self.interactive = False


    def __call__(self):

        self.dataset = self.options.dataset
        self.interactive = commandUsedInsideCrab()

        if not self.rucio:
            self.logger.error("Rucio client not available with this CMSSW version")
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
        locationsMap = self.createLocationsMap(blocks)
        (nbFORnr, nbFORrse) = self.createBlockMaps(locationsMap=locationsMap, blackList=[])
        self.printBlocksPerReplicaMap(nbFORnr)

        self.logger.info("\n Block locations:")
        for rse in nbFORrse.keys():
            msg = f" {rse:20} hosts {nbFORrse[rse]:3} blocks"
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
            self.logger.info(f"Checking disk availability of dataset: {name}")
        else:
            self.logger.info(f"Checking disk availability of container: {name}")
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
            dss = self.rucio.list_content(scope=scope, name=name)
            blocks = [ds['name'] for ds in dss]
            self.logger.info(f"dataset has {len(blocks)} blocks")

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
        cmd = f"curl -s {usableSitesUrl}"
        out, err, ec = execute_command(cmd)
        if not ec:
            usableSites = json.loads(out)
        else:
            self.logger.error(f"EROR retrieving list of blacklisted sites:\n{err}")
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
        nb = 0
        for blockName in blocks:
            nb += 1
            if self.interactive:
                print(f'  block: {nb}', end='\r')
            replicas = set()
            response = self.rucio.list_dataset_replicas(scope='cms', name=blockName, deep=True)
            for item in response:
                if 'Tape' in item['rse']:
                    continue  # skip tape locations
                if 'T3_CH_CERN_OpenData' in item['rse']:
                    continue  # ignore OpenData until it is accessible by CRAB
                if item['state'].upper() == 'AVAILABLE':  # means all files in the block are on disk
                    replicas.add(item['rse'])
            locationsMap[blockName] = replicas
        if self.interactive:
            print("")
        return locationsMap

    def printBlocksPerReplicaMap(self, nbFORnr):
        """
        print how many blocks have 0, 1, 2... disk replicas
        """
        nBlocks = 0
        for nr in sorted(list(nbFORnr.keys())):
            nBlocks += nbFORnr[nr]
            msg = f" {nbFORnr[nr]:3} blocks have {nr:2} disk replicas"
            if nr == 0 and nbFORnr[0] > 0:
                msg += " *THESE BLOCKS WILL NOT BE ACCESSIBLE*"
            self.logger.info(msg)
        if not nbFORnr[0]:
            self.logger.info(" Dataset is fully available")
        else:
            nAvail = nBlocks - nbFORnr[0]
            self.logger.info(f" Only {nAvail}/{nBlocks} are available")
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

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.dataset is None:
            msg = ("%sError%s: Please specify the dataset to check."  # pylint: disable=consider-using-f-string
                   % (colors.RED, colors.NORMAL))
            msg += " Use the --dataset option."
            ex = MissingOptionException(msg)
            ex.missingOption = "dataset"
            raise ex
