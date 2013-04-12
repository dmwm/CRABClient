from CRABClient.client_exceptions import PanDaException
 
import os
import re
import sys
import time
import stat
import types
import random
import urllib
import struct
import commands
import cPickle as pickle
import xml.dom.minidom
import socket
import tempfile

# configuration
try:
    baseURL = os.environ['PANDA_URL']
except:
    baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    baseURLSSL = 'https://voatlas249.cern.ch:25443/server/panda'

baseURLCSRVSSL = "https://voatlas178.cern.ch:25443/server/panda"

# exit code
EC_Failed = 255

globalTmpDir = ''

def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    raise PanDaException("No valid grid proxy certificate found")


# look for a CA certificate directory
def _x509_CApath():
    # use X509_CERT_DIR
    try:
        return os.environ['X509_CERT_DIR']
    except:
        pass
    # get X509_CERT_DIR
    #gridSrc = _getGridSrc()
    #com = "%s echo $X509_CERT_DIR" % gridSrc
    #tmpOut = commands.getoutput(com)
    #return tmpOut.split('\n')[-1]
    return '/etc/grid-security/certificates'



# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl" '
        # verification of the host certificate
        self.verifyHost = True
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # verbose
        self.verbose = False

    # GET method
    def get(self,url,data,rucioAccount=False):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
            com += ' --cacert %s' %  _x509()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # add rucio account info
        if rucioAccount:
            if os.environ.has_key('RUCIO_ACCOUNT'):
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if os.environ.has_key('RUCIO_APPID'):
                data['appid'] = os.environ['RUCIO_APPID']
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print strData[:-1]
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret

    def post(self,url,data,rucioAccount=False):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
            com += ' --cacert %s' %  _x509()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # add rucio account info
        if rucioAccount:
            if os.environ.has_key('RUCIO_ACCOUNT'):
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if os.environ.has_key('RUCIO_APPID'):
                data['appid'] = os.environ['RUCIO_APPID']
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print strData[:-1]
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret

    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
            com += ' --cacert %s' %  _x509()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # emulate PUT 
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        if self.verbose:
            print com
        # execute
        ret = commands.getstatusoutput(com)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # convert return
    def convRet(self,ret):
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        # add messages to silent errors
        if ret[0] == 35:
            ret = (ret[0],'SSL connect error. The SSL handshaking failed. Check grid certificate/proxy.')
        elif ret[0] == 7:
            ret = (ret[0],'Failed to connect to host.')
        elif ret[0] == 55:
            ret = (ret[0],'Failed sending network data.')
        elif ret[0] == 56:
            ret = (ret[0],'Failure in receiving network data.')
        return ret


def putFile(file,verbose=False,useCacheSrv=False,reuseSandbox=False):
    # size check for noBuild
    sizeLimit = 10*1024*1024

    fileSize = os.stat(file)[stat.ST_SIZE]
    if not file.startswith('sources.'):
        if fileSize > sizeLimit:
            errStr  = 'Exceeded size limit (%sB >%sB). ' % (fileSize,sizeLimit)
            errStr += 'Your working directory contains too large files which cannot be put on cache area. '
            errStr += 'Please submit job without --noBuild/--libDS so that your files will be uploaded to SE'
            # get logger
            raise PanDaException(errStr)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # check duplicationn
    if reuseSandbox:
        # get CRC
        fo = open(file)
        fileContent = fo.read()
        fo.close()
        footer = fileContent[-8:]
        checkSum,isize = struct.unpack("II",footer)
        # check duplication
        url = baseURLSSL + '/checkSandboxFile'
        data = {'fileSize':fileSize,'checkSum':checkSum}
        status,output = curl.post(url,data)
        if status != 0:
            raise PanDaException('ERROR: Could not check Sandbox duplication with %s' % status)
        elif output.startswith('FOUND:'):
            # found reusable sandbox
            hostName,reuseFileName = output.split(':')[1:]
            # set cache server hostname
            global baseURLCSRV
            baseURLCSRV    = "http://%s:25080/server/panda" % hostName
            global baseURLCSRVSSL
            baseURLCSRVSSL = "https://%s:25443/server/panda" % hostName
            # return reusable filename
            return 0,"NewFileName:%s:%s" % (hostName, reuseFileName)
    # execute
    if useCacheSrv:
        url = baseURLCSRVSSL + '/putFile'
    else:
        url = baseURLSSL + '/putFile'
    data = {'file':file}
    status, output = curl.put(url, data)
    filename = ""
    servername = ""
    if status !=0:
        raise PanDaException("Failure uploading input file into PanDa (status = %s)" % status)
    else:
       matchURL = re.search("(http.*://[^/]+)/", baseURLCSRVSSL)
       return 0, "True:%s:%s" % (matchURL.group(1), file.split('/')[-1]) 
    #return curl.put(url,data)


def wrappedUuidGen():
    # check if uuidgen is available
    tmpSt,tmpOut = commands.getstatusoutput('which uuidgen')
    if tmpSt == 0:
        # use uuidgen
        return commands.getoutput('uuidgen 2>/dev/null')
    else:
        # use python uuidgen
        try:
            import uuid
        except:
            raise ImportError,'uuidgen and uuid.py are unavailable on your system. Please install one of them'
        return str(uuid.uuid4())
