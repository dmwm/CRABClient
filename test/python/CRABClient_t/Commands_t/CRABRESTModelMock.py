from WMCore.WebTools.RESTModel import RESTModel
import WMCore

import threading
import cherrypy
import imp
import os
import uuid

SI_RESULT = {}
SI_RESULT['server_dn']  = ''
SI_RESULT['my_proxy'] = 'myproxy.cern.ch'

FILE_NAME = 'src_output.root'
goodLumisResult = '{"1":[ [1,15],  [30,50] ], "3":[ [10,15], [30,50] ]}'


class CRABRESTModelMock(RESTModel):
    def __init__(self, config={}):
        RESTModel.__init__(self, config)

        self.mapme = imp.load_source('', os.path.join( os.path.dirname(__file__), "../../../data/mapper.py"))

        self.defaulturi = self.mapme.defaulturi

        self._addMethod('POST', 'user', self.addNewUser,
                        args=[],
                        validation=[self.isalnum])

        self._addMethod('POST', 'task', self.postRequest,
                        args=['requestName'],
                        validation=[self.isalnum])

        self._addMethod('DELETE', 'task', self.deleteRequest,
                        args=['requestID'],
                        validation=[self.isalnum])

        self._addMethod('GET', 'task', self.getTaskStatus,
                        args=['requestID'],
                        validation=[self.isalnum])
        #/data
        self._addMethod('GET', 'data', self.getDataLocation,
                       args=['requestID','jobRange'], validation=[self.isalnum])

        #/goodLumis
        self._addMethod('GET', 'goodLumis', self.getGoodLumis,
                       args=['requestID'], validation=[self.isalnum])
        #
        self._addMethod('POST', 'lumiMask', self.postLumiMask,
                       args=[], validation=[self.isalnum])

        # Server
        self._addMethod('GET', 'info', self.getServerInfo,
                        args=[],
                        validation=[self.isalnum])

        self._addMethod('GET', 'requestmapping', self.getClientMapping,
                        args=[],
                        validation=[self.isalnum])

        self._addMethod('GET', 'jobErrors', self.getJobErrors,
                        args=['requestID'],
                        validation=[self.isalnum])

        cherrypy.engine.subscribe('start_thread', self.initThread)


    #not sure if we really need to validate input.
    def isalnum(self, call_input):
        """
        Validates that all input is alphanumeric, with spaces and underscores
        tolerated.
        """
        for v in call_input.values():
            WMCore.Lexicon.identifier(v)
        return call_input



    def initThread(self, thread_index):
        """
        The ReqMgr expects the DBI to be contained in the Thread
        """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi


    def getServerInfo(self):
        """
        Return information to allow client operations
        """

        return SI_RESULT


    def getTaskStatus(self, requestID):
        return {u'states': {u'/mmascher_crab_sanitized_110930_113018/Analysis': {u'success': {u'count': 5, u'jobs': [41, 42, 43, 44, 45]}}}, \
                           u'requestDetails': {u'percent_success': 0, 'RequestStatus': 'running'}}


    def getDataLocation(self, requestID, jobRange):
        f = open(FILE_NAME, 'w')
        f.close()
        return { '20' : {'pfn' : FILE_NAME } }


    def getGoodLumis(self, requestID):
        """
        Mockup to return the list of good lumis processed as generated
        by CouchDB
        """
        return goodLumisResult


    def getClientMapping(self):
        """
        Return the dictionary that allows the client to map the client configuration to the server request
        It also returns the URI for each API
        """

        return self.defaulturi

    def deleteRequest(self, requestID):
        return {"result": "ok"}


    def addNewUser(self):
        return { "hn_name" : "mmascher" }


    def postRequest(self, requestName):
          return {'ID': 'mmascher_crab_MyAnalysis26_110707_164957'}

    def postLumiMask(self):
        """
        Mock version of result of ACDC upload
        """

        result = {}
        result['DocID']  = uuid.uuid4().hex
        result['DocRev'] = uuid.uuid4().hex
        result['Name']   = "%s-cmsRun1" % params['RequestName']

        return result


    def getJobErrors(self, requestID):
        # { jobid : { retry : { step : [ details: '', type: '', exitCode: '']}}}
        failed = {'1':
                   {'0': {
                     'step1': [ { "details": "Error in StageOut: 99109\n'x.z.root' does not match regular expression /store/temp/([a-zA-Z0-9\\-_]+).root",
                                  "type":"Misc. StageOut error: 99109\n",
                                  "exitCode":99109 }
                              ],
                     'step2': [ { "details": "Cannot find file in jobReport path: /x/y/z/job_134/Report.1.pkl",
                                  "type":"99999",
                                  "exitCode":84 }
                              ]
                     }
                   },
                  '2':
                   {'0': {
                     'step1': [ { "details": "Error in StageOut: 99109\n'x.z.root' does not match regular expression /store/temp/([a-zA-Z0-9\\-_]+).root",
                                  "type":"Misc. StageOut error: 99109\n",
                                  "exitCode":99109 }
                              ]
                     },
                    '1': {
                     'step1': [ { "details": "Error in StageOut: 99109\n'x.z.root' does not match regular expression /store/temp/([a-zA-Z0-9\\-_]+).root",                                                           "type":"Misc. StageOut error: 99109\n",
                                  "exitCode":99109 }
                              ]
                     }
                   }
                 }
        return failed
