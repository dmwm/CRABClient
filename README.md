**CRABClient**

Command Line client for the CRABServer REST interface.

BRANCHES as of August 30 2021
* master
  * for porting to python3, no WMCore etc.
* v3.210607patch
  * forked from tag v3.210607 which is now in production
  * to push important, needed patched to production but aim is to keep stable while working on py3
* py2only
   * created from v3.210607patch plus a couple fixes. Stable release which only works with python2 CMSSW (i.e. < CMSSW_12)
