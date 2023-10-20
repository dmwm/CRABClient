"""
Module to handle lumiMask.json file
"""
import sys
if sys.version_info >= (3, 0):
    from urllib.parse import urlparse  # pylint: disable=E0611
if sys.version_info < (3, 0):
    from urlparse import urlparse

try:
    from FWCore.PythonUtilities.LumiList import LumiList
except Exception:  # pylint: disable=broad-except
    # if FWCore version is not py3 compatible, use our own
    from CRABClient.LumiList import LumiList

from CRABClient.ClientExceptions import ConfigurationException


def getLumiList(lumi_mask_name, logger = None):
    """
    Takes a lumi-mask and returns a LumiList object.
    lumi-mask: either an http address or a json file on disk.
    """
    lumi_list = None
    parts = urlparse(lumi_mask_name)
    if parts[0] in ['http', 'https']:
        if logger:
            logger.debug('Downloading lumi-mask from %s' % lumi_mask_name)
        try:
            lumi_list = LumiList(url = lumi_mask_name)
        except Exception as err:
            raise ConfigurationException("CMSSW failed to get lumimask from URL. Please try to download the lumimask yourself and point to it in crabConfig;\n%s" % str(err))
    else:
        if logger:
            logger.debug('Reading lumi-mask from %s' % lumi_mask_name)
        try:
            lumi_list = LumiList(filename = lumi_mask_name)
        except IOError as err:
            raise ConfigurationException("Problem loading lumi-mask file; %s" % str(err))

    return lumi_list


def getRunList(myrange):
    """
    Take a string like '1,2,5-8' and return a list of integers [1,2,5,6,7,8].
    """
    myrange = myrange.replace(' ','')
    if not myrange:
        return []

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
