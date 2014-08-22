"""
    Used to perform dependency injection necessary for testing
"""

import RESTInteractions
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
defaultDict = {'rest' : RESTInteractions.HTTPRequests,
               'ufc' : UserFileCache}
overrideDict = {}

def clearEmulators():
    global overrideDict
    overrideDict = {}

def getEmulator(name):
    global overrideDict, defaultDict
    if name in overrideDict:
        return overrideDict[name]
    else:
        return defaultDict[name]

def setEmulator(name, value):
    global overrideDict
    overrideDict[name] = value
