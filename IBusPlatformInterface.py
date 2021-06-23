import requests, re, json, sys, os, datetime, logging

from keycloak import KeycloakOpenID
from config import VERSION1, INPUT_JSON_FILE_NAME

class IBusPlatformInterface:
    
    outputModelJson = {}
    iBusLogger = None
    digimopInputParameterJson = None
    debugMsgReq = None
    iBusEdge = False
    logFileName = 'digimop_execution.log'
    logFileHandle = None
    
    def __init__(self, version, debugMsgReq, digimopInputParameterJson):
        self.version = version
        if digimopInputParameterJson == None:
            self.digimopInputParameterJson = self.__read_input_file(INPUT_JSON_FILE_NAME)
        else:
            self.digimopInputParameterJson = digimopInputParameterJson
        self.__readInputParametersFromJsonObj(self.digimopInputParameterJson)
        self.debugMsgReq = debugMsgReq
        self.logFileName = 'id_'+str(self.workflowHistoryId)+'_'+self.logFileName
        
    def __reInitializeLog(self):
        print ("Execution happening for History Id : "+ str(self.workflowHistoryId))
        if not os.path.exists(self.logFileName) or self.logFileHandle == None:
            self.logFileHandle = open(self.logFileName,"a+")
            
    def uploadOutputJsonFile(self):
        if self.version == VERSION1:
            print(self.outputModelJson)
            return self.__uploadOutputJsonFileV1()
    
    def getInputParameterFile(self, parameter, filename=None):
        if self.version == VERSION1:
            return self.__getInputParameterFileV1(parameter, filename)
        
    def uploadOutputParameterFile(self, parameter, completeFilePath):
        if self.version == VERSION1:
            return self.__uploadOutputParameterFileV1(parameter, completeFilePath)

    def uploadLogFile(self):
        if self.version == VERSION1:
            return self.__uploadLogFileV1()
        
    def uploadLiveLog(self, level, message):
        if self.version == VERSION1:
            return self.__uploadLiveLogV1(level, str(message))
    
    def getInputJsonData(self):
        return self.digimopInputParameterJson
    
    def __read_input_file(self, inputJsonFile):
        with open(str(inputJsonFile)) as json_file:
            data = json.load(json_file)
        return data;
    
    def __readKeycloakParametersFromJsonObj(self, digimopInputParameterJson):
        self.keycloakServiceUrl = digimopInputParameterJson['keyclock-auth-service-url'] + '/'
        self.keycloakClient = digimopInputParameterJson['keyclock-client-id']
        self.keycloakRealm = digimopInputParameterJson['keyclock-realm']
        self.keycloakCltSecret = digimopInputParameterJson['keyclock-client-secret']
        self.keycloakClientUser = digimopInputParameterJson['keyclock-client-user']
        self.keycloakClientPassword = digimopInputParameterJson['keyclock-client-password']
        self.dgimopLoggerServiceUrl = digimopInputParameterJson['digimop-logger-service-base-url']
        
    def __readInputParametersFromJsonObj(self, digimopInputParameterJson):
        self.profile = digimopInputParameterJson['profile']
        if self.profile == 'edge':
            self.iBusEdge = True
        
        if not self.iBusEdge:
            self.__readKeycloakParametersFromJsonObj(digimopInputParameterJson)
        self.digimopOperationId = digimopInputParameterJson['digimop-operation-id']
        self.workflowHistoryId = digimopInputParameterJson['workflow-instance-history-id']
        self.workflowInstanceId = digimopInputParameterJson['workflow-instance-id']
        self.workflowManagerServiceUrl = digimopInputParameterJson['workflow-manager-service-base-url']
        
    def __getKeycloakToken(self):
#         print("KEYCLOAK : Url={} user={}, password={}, ClientID={}, Realm={}, Client Secret={}".format(self.keycloakServiceUrl, self.keycloakClientUser, self.keycloakClientPassword, self.keycloakClient, self.keycloakRealm, self.keycloakCltSecret))
        keycloak_openid = KeycloakOpenID(server_url=self.keycloakServiceUrl,
                                             client_id=self.keycloakClient,
                                             realm_name=self.keycloakRealm,
                                             client_secret_key=self.keycloakCltSecret)
        keycloakToken = keycloak_openid.token(username=self.keycloakClientUser,
                                          password=self.keycloakClientPassword)
        return keycloakToken["access_token"]
    
    def __uploadLiveLogV1(self, level, message):
        if not self.iBusEdge:
            liveLogUploadUrl = (self.dgimopLoggerServiceUrl + '/api/digimop-logger/v1/log')
            try:
    #             print("UPLOAD_LIVE_LOG : URL ={}".format(liveLogUploadUrl))
                today = datetime.datetime.now()
                currentTimestamp = today.strftime("%Y-%m-%dT%H:%M:%S%ZZ")
                liveLogJson = {"workflowInstanceId" : self.workflowInstanceId, "digimopOperationId" : self.digimopOperationId, "timestamp" : str(currentTimestamp), "severity" : str(level), "message" : str(message)}
                postReqHeaders = {"authorization": "Bearer " + self.__getKeycloakToken(), "content-type": "application/json"}
                response = requests.post(url=liveLogUploadUrl, data=json.dumps(liveLogJson), headers=postReqHeaders)
                print("UPLOAD_LIVE_LOG : Status Code ={}".format(response.status_code))
                if response.status_code == 200:
                    return True
                else:
                    return False
            except Exception as e:
                print("UPLOAD_LIVE_LOG : Error in uploading output json file: " + str(e))
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print("Exception Type : {} at line {}".format(exc_type, exc_tb.tb_lineno));
            
    def __uploadLogFileV1(self):
        logFileUploadUrl = (self.workflowManagerServiceUrl + '/api/workflow-manager/v1/digimop/operation/{}/workflow/instance/{}/file/{}/workflowInstanceHistoryId/{}'.format(self.digimopOperationId, self.workflowInstanceId, 'log', self.workflowHistoryId))
        try:
            if not self.logFileHandle == None:
                self.logFileHandle.close();
            file_with_path = os.path.realpath(self.logFileName)
            print ("file  exist "+ file_with_path + " " + str(os.path.exists(file_with_path)))
            postReqFiles = {'file': open(file_with_path, 'rb')}
            if self.iBusEdge:
                response = requests.post(logFileUploadUrl, files=postReqFiles)
            else:
                postReqHeaders = {"authorization": "Bearer " + self.__getKeycloakToken()}
                response = requests.post(logFileUploadUrl, headers=postReqHeaders, files=postReqFiles)
            print("UPLOAD_LOG_FILE : status_code ={}".format(response.status_code))
            if os.path.exists(file_with_path):
                os.remove(file_with_path)
            if response.status_code == 200:
                return True
            else:
                print('UPLOAD_LOG_FILE : response text = ' + response.text)
                return False
        except Exception as e:
            print("UPLOAD_LOG_FILE : Error in uploading log file: " + str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Exception Type : {} at line - {}".format(exc_type, exc_tb.tb_lineno));

    def __uploadOutputJsonFileV1(self):
        outputJsonFileUploadUrl = (self.workflowManagerServiceUrl + '/api/workflow-manager/v1/digimop/operation/json/output?digimopOperationId={}&workflowInstanceId={}'.format(self.digimopOperationId, self.workflowInstanceId))
        print("UPLOAD_OUTPUT_JSON_FILE : url ={}".format(outputJsonFileUploadUrl))
        try:
            if self.iBusEdge:
                postReqHeaders = {"content-type": "application/json"}
            else:
                postReqHeaders = {"authorization": "Bearer " + self.__getKeycloakToken(), "content-type": "application/json"}
            response = requests.post(url=outputJsonFileUploadUrl, data=json.dumps(self.outputModelJson), headers=postReqHeaders)
            print("UPLOAD_OUTPUT_JSON_FILE : status_code ={}".format(response.status_code))
            if response.status_code == 200:
                return True
            else:
                print('response text = ' + response.text)
                return False
        except Exception as e:
            print("UPLOAD_OUTPUT_JSON_FILE : Error in uploading output json file: " + str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Exception Type : {} at line - {}".format(exc_type, exc_tb.tb_lineno));
    
    def __getInputParameterFileV1(self, parameter, filename=None):
        inputFileDownloadUrl = (self.workflowManagerServiceUrl + '/api/workflow-manager/v1/digimop/operation/{}/workflow/instance/{}/file/{}/parameter/'.format(self.digimopOperationId, self.workflowInstanceId, 'input'))
        try:
            url = inputFileDownloadUrl + parameter
            if self.iBusEdge:
                response = requests.get(url)
            else:
                getReqHeaders = {"authorization": "Bearer " + self.__getKeycloakToken()}
                response = requests.get(url, headers=getReqHeaders)
                
            print("GET_INPUT_PARAMETER_FILE : status_code ={}".format(response.status_code))
            if response.status_code == 200:
                if not filename and "Content-Disposition" in response.headers.keys():
                    filename = re.findall("filename=(.+)", response.headers["Content-Disposition"])[0]
                with open(filename, 'wb') as file:
                    file.write(response.content)
                return True
            else:
                print('GET_INPUT_PARAMETER_FILE : response text = ' + response.text)
                return False
        except Exception as e:
            print("GET_INPUT_PARAMETER_FILE : Error in downloading input parameter file: " + str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Exception Type : {} at line {}".format(exc_type, exc_tb.tb_lineno));
            
    def __uploadOutputParameterFileV1(self, parameter, completeFilePath):
        outputFileUploadUrl = (self.workflowManagerServiceUrl + '/api/workflow-manager/v1/digimop/operation/{}/workflow/instance/{}/file/{}/parameter/'.format(self.digimopOperationId, self.workflowInstanceId, 'output'))
        try:
            url = outputFileUploadUrl + parameter
            file_with_path = os.path.realpath(completeFilePath)
            print("UPLOAD_OUTPUT_PARAMETER_FILE : File to be uploaded ={}".format(file_with_path))
            postReqFiles = {'file': open(file_with_path, 'rb')}
            
            if self.iBusEdge:
                response = requests.post(url, files=postReqFiles)
            else:
                postReqHeaders = {"authorization": "Bearer " + self.__getKeycloakToken()}
                response = requests.post(url, headers=postReqHeaders, files=postReqFiles)
                
            print("UPLOAD_OUTPUT_PARAMETER_FILE : status_code ={}".format(response.status_code))
            if response.status_code == 200:
                return True
            else:
                print('UPLOAD_OUTPUT_PARAMETER_FILE : response text = ' + response.text)
                return False
        except Exception as e:
            print("UPLOAD_OUTPUT_PARAMETER_FILE : Error in uploading output parameter file: " + str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Exception Type : {} at line {}".format(exc_type, exc_tb.tb_lineno));
            
    def logInfo(self, message):
        self.__reInitializeLog()
        self.uploadLiveLog("INFO", str(message))
        self.logFileHandle.write(str(datetime.datetime.now()) + " INFO "+str(message)+ " \n")
        
    def logWarning(self, message):
        self.__reInitializeLog()
        self.uploadLiveLog("WARN", str(message))
        self.logFileHandle.write(str(datetime.datetime.now()) + " WARN "+ str(message)+ " \n")
        
    def logError(self, message):
        self.__reInitializeLog()
        self.uploadLiveLog("ERROR", str(message))
        self.logFileHandle.write(str(datetime.datetime.now()) + " ERROR " + str(message)+ " \n")

    def logDebug(self, message):
        self.__reInitializeLog()
        if self.debugMsgReq:
            self.uploadLiveLog("DEBUG", str(message))
            self.logFileHandle.write(str(datetime.datetime.now()) + " DEBUG " + str(message)+ " \n")
    
if __name__ == "__main__":
    iBusObj = IBusPlatformInterface(sys.argv[1], sys.argv[2], None)
    if sys.argv[3] == "uploadOutputJsonFile":
        iBusObj.outputModelJson = json.loads(sys.argv[4])
        iBusObj.uploadOutputJsonFile()

    if sys.argv[3] == "getInputParameterFile":
        iBusObj.getInputParameterFile(sys.argv[4])
        
    if sys.argv[3] == "uploadOutputParameterFile":
        iBusObj.uploadOutputParameterFile(sys.argv[4], sys.argv[5])
        
    if sys.argv[3] == "uploadLogFile":
        iBusObj.uploadLogFile()

    if sys.argv[3] == "uploadLiveLog":
        if sys.argv[2] == "True":
            iBusObj.debugMsgReq = True
        else:
            iBusObj.debugMsgReq = False
        level = sys.argv[4]
        message = sys.argv[5]
        if level == "info":
            iBusObj.logInfo(str(message))
        if level == "warn":
            iBusObj.logWarning(str(message))
        if level == "error":
            iBusObj.logError(str(message))
        if level == "debug":
            iBusObj.logDebug(str(message))
            
            
