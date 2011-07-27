from WMCore.WebTools.RESTModel import RESTModel
import WMCore
import client_default

import threading
import cherrypy

SI_RESULT = {}
SI_RESULT['server_dn']  = ''
SI_RESULT['my_proxy'] = 'myproxy.cern.ch'
SI_RESULT['sandbox'] = {}
SI_RESULT['sandbox']['type'] = 'gridFtp'
SI_RESULT['sandbox']['endpoint'] = ''
SI_RESULT['sandbox']['port'] = ''
SI_RESULT['sandbox']['basepath'] = ''

FILE_NAME = 'src_output.root'
goodLumisResult = {'1':[ [1,15],  [30,50] ],
                   '3':[ [10,15], [30,50] ],}


class CRABRESTModelMock(RESTModel):
    def __init__(self, config={}):
        RESTModel.__init__(self, config)

        self.defaulturi = {
            'submit' : {'uri': '/unittests/rest/task/',
                        'map': client_default.defaulturi['submit']['map']},
            'getlog' : {'uri': '/unittests/rest/log/'},
            'getoutput' : {'uri': '/unittests/rest/data/'},
            'reg_user' : {'uri': '/unittests/rest/user/'},
            'server_info' : {'uri': '/unittests/rest/info/'},
            'status' : {'uri': '/unittests/rest/task/'},
            'report' :    {'uri': '/unittests/rest/goodLumis/'},
            'get_client_mapping': {'uri': '/unittests/rest/requestmapping/'}
        }

        self._addMethod('POST', 'user', self.addNewUser,
                        args=[],
                        validation=[self.isalnum])

        self._addMethod('POST', 'task', self.postRequest,
                        args=['requestName'],
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

        # Server
        self._addMethod('GET', 'info', self.getServerInfo,
                        args=[],
                        validation=[self.isalnum])

        self._addMethod('GET', 'requestmapping', self.getClientMapping,
                        args=[],
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
        return {u'states': {u'success': {u'count': 5, u'jobs': [41, 42, 43, 44, 45]}}, \
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


    def addNewUser(self):
        return { "hn_name" : "mmascher" }


    def postRequest(self, requestName):
          return {'ID': 'mmascher_crab_MyAnalysis26_110707_164957'}

