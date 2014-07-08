"""
Module to handle lumiMask.json file
"""

import urlparse

from WMCore.Lexicon import jobrange
from WMCore.DataStructs.LumiList import LumiList

from CRABClient.client_exceptions import ConfigurationException


def getLumiList(lumi_mask_name, logger = None):
    """
    Takes a lumi-mask and returns a LumiList object.
    lumi-mask: either an http address or a json file on disk.
    """
    lumi_list = None
    parts = urlparse.urlparse(lumi_mask_name)
    if parts[0] in ['http', 'https']:
        if logger:
            logger.debug('Downloading lumi-mask from %s' % lumi_mask_name)
        lumi_list = LumiList(url = lumi_mask_name)
    else:
        if logger:
            logger.debug('Reading lumi-mask from %s' % lumi_mask_name)
        lumi_list = LumiList(filename = lumi_mask_name)

    return lumi_list


def getRunList(myrange):
    """
    Take a string like '1,2,5-8' and return a list of integers [1,2,5,6,7,8].
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
            result.append(int(element))

    return result
