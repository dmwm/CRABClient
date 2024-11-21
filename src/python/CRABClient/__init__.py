#!/usr/bin/env python
"""
CRAB Client modules
"""
__version__ = "development"

#the __version__ will be automatically be change according to rpm for production

import sys
if (sys.version_info[0]*100+sys.version_info[1])>=312:
    import importlib
    def find_module(moduleName, moduleDir = None):
        if moduleDir:
            return importlib.machinery.PathFinder.find_spec(moduleName, [moduleDir])
        return importlib.machinery.PathFinder.find_spec(moduleName)

    def load_module(moduleName, moduleSpec):
        moduleObj = importlib.util.module_from_spec(moduleSpec)
        sys.modules[moduleName] = moduleObj
        moduleSpec.loader.exec_module(moduleObj)
        return moduleObj

    def load_source(moduleName, moduleFile):
        moduleSpec = importlib.util.spec_from_file_location(moduleName,
                                                            moduleFile)
        return load_module(moduleName, moduleSpec)

    def module_pathname(moduleObj):
        return moduleObj.origin
else:
    import imp
    def find_module(moduleName, moduleDir = None):
        if moduleDir:
            return imp.find_module(moduleName, [moduleDir])
        return imp.find_module(moduleName)

    def load_module(moduleName, moduleObj):
        return imp.load_module(moduleName, moduleObj[0],
                               moduleObj[1], moduleObj[2])

    def load_source(moduleName, moduleFile):
        return imp.load_source(moduleName, moduleFile)

    def module_pathname(moduleObj):
        return moduleObj[1]
