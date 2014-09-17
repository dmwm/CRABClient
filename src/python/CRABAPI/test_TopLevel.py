# pylint: disable=locally-disabled,missing-docstring,too-many-public-methods
# The above is ONLY permissible in test suites
import logging
import unittest
import CRABAPI
class test_TopLevel(unittest.TestCase):
    def test_getTask_notimpl(self):
        self.assertRaises(NotImplementedError, CRABAPI.getTask, "")
    def test_setLogging(self):
        CRABAPI.setLogging(logging.DEBUG, logging.DEBUG, logging.DEBUG)
        log1, log2, log3 = CRABAPI.getAllLoggers('withsuffix')
        log1single = CRABAPI.getLogger('withsuffix')
        self.assertEqual(log1, log1single)
        log1, log2nosuffix, log3nosuffix = CRABAPI.getAllLoggers()
        log1single = CRABAPI.getLogger()
        self.assertEqual(log1, log1single)
        self.assertEqual(log2, log2nosuffix)
        self.assertEqual(log3, log3nosuffix)
