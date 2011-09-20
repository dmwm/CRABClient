"""
Module to handle lumiMask.json file
"""

import json
import urllib2
import urlparse

from ServerInteractions import HTTPRequests


class LumiMask(object):
    """
    Class to handle LumiMask JSON file
    """
    def __init__(self, config, logger=None):
        self.config = config
        self.logger = logger

        # Parse Lumi list from file or URL
        parts = urlparse.urlparse(self.config.Data.lumiMask)
        if parts[0] in ['http', 'https']:
            self.logger.debug('Downloading lumiMask from %s' % self.config.Data.lumiMask)
            lumiFile = urllib2.urlopen(self.config.Data.lumiMask)
            self.lumiMask = json.load(lumiFile)
        else:
            with open(self.config.Data.lumiMask, 'r') as lumiFile:
                self.logger.debug('Reading lumiMask from %s' % self.config.Data.lumiMask)
                self.lumiMask = json.load(lumiFile)


    def upload(self, requestConfig):
        """
        Upload the lumi mask file to the server
        """

        url = self.config.General.serverUrl
        server = HTTPRequests(url)
        if not server:
            raise RuntimeError('No server specified for lumiMask upload')

        group  = self.config.User.group
        userDN = requestConfig['RequestorDN']

        data = {'LumiMask'    : self.lumiMask, 'DatasetName' : self.config.Data.inputDataset,
                'Description' : '',            'RequestName' : requestConfig['RequestName'],
                'Group'       : group,         'UserDN'      : userDN,
                'Label'       : '',
               }
        jsonString = json.dumps(data, sort_keys=False)

        result = server.post(uri='/crabinterface/crab/lumiMask/', data=jsonString)
        self.logger.debug('Result of LumiMask POST: %s' % str(result))
        return result
