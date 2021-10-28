#!/usr/bin/env python
# pylint: disable=C0321,C0103,W0622
# W0622: redefined-builtin

# Original code is taken from WMCore: https://github.com/dmwm/WMCore/blob/4ec90a8d7f2e1902e37003f9391252cab43f08bc/src/python/WMCore/Configuration.py
# Code parts that are not used by CRAB were removed

"""
_Configuration_

Module dealing with Configuration file in python format
"""

import imp
import os
import sys
import traceback

PY3 = sys.version_info[0] == 3

# PY3 compatibility (can be removed once python2 gets dropped)
if PY3:
    basestring = str
    unicode = str
    long = int

_SimpleTypes = [
    bool,
    float,
    basestring,  # For py2/py3 compatibility, don't let futurize remove PY3 Remove when python 3 transition complete
    str,
    long,  # PY3: Not needed in python3, will be converted to duplicate int
    type(None),
    int,
]

_ComplexTypes = [
    dict,
    list,
    tuple,
]

_SupportedTypes = []
_SupportedTypes.extend(_SimpleTypes)
_SupportedTypes.extend(_ComplexTypes)

def formatAsString(value):
    """
    _format_
    format a value as python
    keep parameters simple, trust python...
    """
    if isinstance(value, str):
        value = "\'%s\'" % value
    return str(value)


class ConfigSection(object):
    # pylint: disable=protected-access
    """
    _ConfigSection_

    Chunk of configuration information
    """

    def __init__(self, name=None):
        object.__init__(self)
        self._internal_documentation = ""
        self._internal_name = name
        self._internal_settings = set()
        self._internal_docstrings = {}
        self._internal_children = set()
        self._internal_parent_ref = None
        # The flag skipChecks controls weather each parameter added to the configuration
        # should be a primitive or complex type that can be "jsonized"
        # By default it is false, but ConfigurationEx instances set it to True
        self._internal_skipChecks = False

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return (
                (self._internal_documentation == other._internal_documentation) and
                (self._internal_name == other._internal_name) and
                (self._internal_settings == other._internal_settings) and
                (self._internal_docstrings == other._internal_docstrings) and
                (self._internal_children == other._internal_children) and
                (self._internal_parent_ref == other._internal_parent_ref))
        return id(self) == id(other)

    def _complexTypeCheck(self, name, value):

        if isinstance(value, tuple(_SimpleTypes)):
            return
        elif isinstance(value, tuple(_ComplexTypes)):
            vallist = value
            if isinstance(value, dict):
                vallist = value.values()
            for val in vallist:
                self._complexTypeCheck(name, val)
        else:
            msg = "Not supported type in sequence:"
            msg += "%s\n" % type(value)
            msg += "for name: %s and value: %s\n" % (name, value)
            msg += "Added to Configuration."
            raise RuntimeError(msg)

    def __setattr__(self, name, value):
        if name.startswith("_internal_"):
            # skip test for internal setting
            object.__setattr__(self, name, value)
            return

        if isinstance(value, ConfigSection):
            # child ConfigSection
            self._internal_children.add(name)
            self._internal_settings.add(name)
            value._internal_parent_ref = self
            object.__setattr__(self, name, value)
            return

        if isinstance(value, unicode):
            value = str(value)

        # for backward compatibility use getattr and sure to work if the
        # _internal_skipChecks flag is not set
        if not getattr(self, '_internal_skipChecks', False):
            self._complexTypeCheck(name, value)

        object.__setattr__(self, name, value)
        self._internal_settings.add(name)
        return

    def __iter__(self):
        for attr in self._internal_settings:
            yield getattr(self, attr)

    def section_(self, sectionName):
        """
        _section_

        Get a section by name, create it if not present,
        returns a ConfigSection instance

        """
        if sectionName in self.__dict__:
            return self.__dict__[sectionName]
        newSection = ConfigSection(sectionName)
        self.__setattr__(sectionName, newSection)
        return object.__getattribute__(self, sectionName)

    def pythonise_(self, **options):
        """
        convert self into list of python format strings
        options available
        document - if True will add document_ calls to the python
        comment  - if True will add docs as comments.
        """

        document = options.get('document', False)
        comment = options.get('comment', False)
        prefix = options.get('prefix', None)

        if prefix is not None:
            myName = "%s.%s" % (prefix, self._internal_name)
        else:
            myName = self._internal_name

        result = []
        if document:
            result.append("%s.document_(\"\"\"%s\"\"\")" % (
                myName,
                self._internal_documentation)
                          )
        if comment:
            result.append("# %s: %s" % (
                myName, self._internal_documentation.replace(
                        "\n", "\n# "),
            ))
        for attr in self._internal_settings:
            if attr in self._internal_children:
                result.append("%s.section_(\'%s\')" % (myName, attr))
                result.extend(getattr(self, attr).pythonise_(
                        document=document, comment=comment, prefix=myName))
                continue
            if attr in self._internal_docstrings:
                if comment:
                    result.append("# %s.%s: %s" % (
                        myName, attr,
                        self._internal_docstrings[attr].replace("\n", "\n# ")
                    ))
            result.append("%s.%s = %s" % (
                myName,
                attr, formatAsString(getattr(self, attr))
            ))

            if attr in self._internal_docstrings:
                if document:
                    result.append(
                            "%s.document_(\"\"\"%s\"\"\", \'%s\')" % (
                                myName,
                                self._internal_docstrings[attr], attr))
        return result

    def __str__(self):
        """
        string representation, dump to python format
        """
        result = ""
        for pystring in self.pythonise_():
            result += "%s\n" % pystring
        return result

    def listSections_(self):
        """
        _listSections_

        Retrieve a list of components from the components
        configuration section

        """
        comps = self._internal_settings
        return list(comps)


class Configuration(object):
    # pylint: disable=protected-access
    """
    _Configuration_

    Top level configuration object

    """

    def __init__(self):
        object.__init__(self)
        self._internal_sections = []
        Configuration._instance = self

    def __setattr__(self, name, value):
        if name.startswith("_internal_"):
            # skip test for internal settings
            object.__setattr__(self, name, value)
            return
        if not isinstance(value, ConfigSection):
            msg = "Can only add objects of type ConfigSection to Configuration"
            raise RuntimeError(msg)

        object.__setattr__(self, name, value)
        return


    def listSections_(self):
        """
        _listSections_

        Retrieve a list of components from the components
        configuration section

        """
        comps = self._internal_sections
        return comps

    def section_(self, sectionName):
        """
        _section_

        Get a section by name, create it if not present,
        returns a ConfigSection instance

        """
        if sectionName in self.__dict__:
            return self.__dict__[sectionName]
        newSection = ConfigSection(sectionName)
        self.__setattr__(sectionName, newSection)
        self._internal_sections.append(sectionName)
        return object.__getattribute__(self, sectionName)

    def pythonise_(self, **options):
        """
        write as python format
        document - if True will add document_ calls to the python
        comment  - if True will add docs as comments.
        """

        document = options.get('document', False)
        comment = options.get('comment', False)

        result = "from CRABClient.Configuration import Configuration\n"
        result += "config = Configuration()\n"
        for sectionName in self._internal_sections:
            result += "config.section_(\'%s\')\n" % sectionName

            sectionRef = getattr(self, sectionName)
            for sectionAttr in sectionRef.pythonise_(
                    document=document, comment=comment):
                if sectionAttr.startswith("#"):
                    result += "%s\n" % sectionAttr
                else:
                    result += "config.%s\n" % sectionAttr

        return result

    def __str__(self):
        """
        string format of this object
        """

        return self.pythonise_()


def loadConfigurationFile(filename):
    """
    _loadConfigurationFile_

    Load a Configuration File
    """

    cfgBaseName = os.path.basename(filename).replace(".py", "")
    cfgDirName = os.path.dirname(filename)
    if not cfgDirName:
        modPath = imp.find_module(cfgBaseName)
    else:
        modPath = imp.find_module(cfgBaseName, [cfgDirName])
    try:
        modRef = imp.load_module(cfgBaseName, modPath[0],
                                 modPath[1], modPath[2])
    except Exception as ex:
        msg = "Unable to load Configuration File:\n"
        msg += "%s\n" % filename
        msg += "Due to error:\n"
        msg += str(ex)
        msg += str(traceback.format_exc())
        raise RuntimeError(msg)

    for attr in modRef.__dict__.values():
        if isinstance(attr, Configuration):
            return attr

    msg = "Unable to find a Configuration object instance in file:\n"
    msg += "%s\n" % filename
    raise RuntimeError(msg)

