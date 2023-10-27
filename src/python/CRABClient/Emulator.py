# pylint: disable=global-statement
"""
    Used to perform dependency injection necessary for testing
"""

defaultDict = None
overrideDict = {}

def clearEmulators():
    global overrideDict
    overrideDict = {}

def getEmulator(name):
    global overrideDict, defaultDict
    if name in overrideDict:
        return overrideDict[name]
    else:
        if not defaultDict:
            defaultDict = getDefaults()
        return defaultDict[name]

def setEmulator(name, value):
    global overrideDict
    overrideDict[name] = value

def getDefaults():
    import CRABClient.RestInterfaces
    return {'rest' : CRABClient.RestInterfaces.CRABRest,
            'ufc' : 'dummy_ufc'}
