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

        """
        #/user
        self._addMethod('POST', 'user', self.addNewUser,
                        args=[],
                        validation=[self.isalnum])
        #/task
        """
        self._addMethod('GET', 'task', self.getTaskStatus,
                        args=['requestID'],
                        validation=[self.isalnum])
        """
        self._addMethod('PUT', 'task', self.putTaskModifies,
                        args=['requestID'],
                        validation=[self.isalnum])
        self._addMethod('DELETE', 'task', self.deleteRequest,
                        args=['requestID'],
                        validation=[self.isalnum])
        self._addMethod('POST', 'task', self.postRequest,
                        args=['requestName'],
                        validation=[self.isalnum])
        #/config
        self._addMethod('POST', 'config', self.postUserConfig,
                        args=[],
                        validation=[self.checkConfig])
        """
        #/data
        self._addMethod('GET', 'data', self.getDataLocation,
                       args=['requestID','jobRange'], validation=[self.isalnum])
        """
        #/log
        self._addMethod('GET', 'log', self.getLogLocation,
                       args=['requestID','jobRange'], validation=[self.checkConfig])
        """

        # Server
        self._addMethod('GET', 'info', self.getServerInfo,
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

