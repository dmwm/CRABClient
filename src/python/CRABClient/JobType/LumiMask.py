"""
Module to handle lumiMask.json file
"""

import json
import urllib2
import urlparse

from WMCore.Lexicon import jobrange

from CRABClient.client_exceptions import ConfigurationException


def getLumiMask(config, logger=None):
    """
    Takes the lumiMask and runRange parameter and return the lumiMask dict.
    lumiMask: either an http address or a json file on the disk
    runRange: a string range like '1,2,5-8' containing the runs the user are
             interested to. Runs in lumiMask are filtered according to runRange
    returns: a dict in the json format
             (e.g.: {'1':[[3,5],[7,9]],'4':[[1,1],[5,10]]})
    """
    # Parse Lumi list from file or URL
    parts = urlparse.urlparse(config.Data.lumiMask)
    if parts[0] in ['http', 'https']:
        logger.debug('Downloading lumiMask from %s' % config.Data.lumiMask)
        lumiFile = urllib2.urlopen(config.Data.lumiMask)
        lumiMask = json.load(lumiFile)
    else:
        with open(config.Data.lumiMask, 'r') as lumiFile:
            logger.debug('Reading lumiMask from %s' % config.Data.lumiMask)
            lumiMask = json.load(lumiFile)

    runRange = _expandRange( getattr(config.Data, 'runRange', ''))

    return dict((run, lumi) for run, lumi in lumiMask.iteritems() if not runRange or run in runRange)



def _expandRange(myrange):
    """
    Used to expand the runRange parameter
    Take a string like '1,2,5-8' and return a list of integers [1,2,5,6,7,8]
    """
    myrange = myrange.replace(' ','')
    if not myrange:
        return []
    try:
        jobrange(myrange)
    except AssertionError, ex:
        raise ConfigurationException("Invalid runRange: %s" % myrange)

    myrange = myrange.split(',')
    result = []
    for element in myrange:
        if element.count('-') > 0:
            mySubRange = element.split('-')
            subInterval = range( int(mySubRange[0]), int(mySubRange[1])+1)
            result.extend(subInterval)
        else:
            restult.append(int(element))

    return result
