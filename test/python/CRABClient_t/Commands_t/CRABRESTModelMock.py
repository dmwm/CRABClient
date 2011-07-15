from WMCore.WebTools.RESTModel import RESTModel
import WMCore

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


class CRABRESTModelMock(RESTModel):
    def __init__(self, config={}):
        RESTModel.__init__(self, config)

        self.defaulturi = {
            'submit' : {'uri': '/unittests/rest/task/'},
            'getlog' : {'uri': '/unittests/rest/log/'},
            'getoutput' : {'uri': '/unittests/rest/data/'},
            'reg_user' : {'uri': '/unittests/rest/user/'},
            'server_info' : {'uri': '/unittests/rest/info/'},
            'status' : {'uri': '/unittests/rest/task/'},
            'get_client_mapping': {'uri': '/unittests/rest/requestmapping/'}
        }

        self._addMethod('GET', 'task', self.getTaskStatus,
                        args=['requestID'],
                        validation=[self.isalnum])
        #/data
        self._addMethod('GET', 'data', self.getDataLocation,
                       args=['requestID','jobRange'], validation=[self.isalnum])

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
        return {u'percent_success': 100.0, 'RequestStatus': 'running'}


    def getDataLocation(self, requestID, jobRange):
        return { '20' : 'src_outputt.root' }


    def getClientMapping(self):
        """
        Return the dictionary that allows the client to map the client configuration to the server request
        It also returns the URI for each API
        """

        return self.defaulturi

