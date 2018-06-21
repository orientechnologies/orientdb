@if (@X) == (@Y) @end /* JScript comment 
        @echo off 
        
        rem :: the first argument is the script name as it will be used for proper help message 
        cscript //E:JScript //nologo "%~f0" "%~nx0" %* 

        exit /b %errorlevel% 
        
@if (@X)==(@Y) @end JScript comment */


var ARGS = WScript.Arguments;
var scriptName = ARGS.Item(0);

var url = "";

var user = 0;
var pass = 0;
var database = 0;
var databaseUser = 0;
var databasePass = 0;
var baseUrl = 0;
var basePort = 0;
var companyCode = 0;

//ActiveX objects 
var WinHTTPObj = new ActiveXObject("WinHttp.WinHttpRequest.5.1");
var FileSystemObj = new ActiveXObject("Scripting.FileSystemObject");
var AdoDBObj = new ActiveXObject("ADODB.Stream");

// 
HTTPREQUEST_SETCREDENTIALS_FOR_SERVER = 0;
HTTPREQUEST_SETCREDENTIALS_FOR_PROXY = 1;

//timeouts and their default values 
var RESOLVE_TIMEOUT = 0;
var CONNECT_TIMEOUT = 90000;
var SEND_TIMEOUT = 90000;
var RECEIVE_TIMEOUT = 90000;

//autologon policy 
var autologon_policy = 1; //0,1,2 


//headers will be stored as multi-dimensional array 
var headers = [];

function parseArgs() {
    // 
    if (ARGS.Length < 4) {               
        WScript.Quit(43);
    }
    // !!! 
    url = "http://" + ARGS.Item(1) + ":" + ARGS.Item(2);
    // !!!     
	
	baseUrl = ARGS.Item(1);
	basePort = ARGS.Item(2);

    user = ARGS.Item(3);
	pass = ARGS.Item(4).toString();	
	database = ARGS.Item(5);
	databaseUser = ARGS.Item(6);
	databasePass = ARGS.Item(7);
	companyCode = ARGS.Item(8);
}



//------------------------------- 
//---------------------- 
//---------- 
//----- 
//-- 
function request(functionUrl) {
    var responseText = "";
    var status;
    try {		
        WinHTTPObj.Open(http_method, functionUrl, false);
				
        if (user !== 0 && pass !== 0) {			
            WinHTTPObj.SetCredentials(user, pass, HTTPREQUEST_SETCREDENTIALS_FOR_SERVER);
        }
	    else{
			WScript.Echo("User or pass is null");
		}

        //set autologin policy 
        WinHTTPObj.SetAutoLogonPolicy(autologon_policy);
        //set timeouts 
        WinHTTPObj.SetTimeouts(RESOLVE_TIMEOUT, CONNECT_TIMEOUT, SEND_TIMEOUT, RECEIVE_TIMEOUT);

        if (headers.length !== 0) {            
            for (var i = 0; i < headers.length; i++) {				
                WinHTTPObj.SetRequestHeader(headers[i][0], headers[i][1]);
            }
        }        
        
		if (postBody != 0){
			WinHTTPObj.Send(postBody);
		}
		else{
			WinHTTPObj.Send();
		}
        status = WinHTTPObj.Status
    } catch (err) {
        WScript.Echo(err.message);
        WScript.Quit(666);
    }
    

	if (Math.floor(status / 100) != 2){
		WScript.Echo("Status: " + status);
		WScript.Echo("Status was not OK. More info -> https://en.wikipedia.org/wiki/List_of_HTTP_status_codes");
	}
	
	responseText = WinHTTPObj.ResponseText;
	
    return responseText;
}

//-- 
//----- 
//---------- 
//---------------------- 
//------------------------------- 

function getFunctionId(response){
	var command = 'obj = ' + response;
	eval(command);
	var id = obj["@rid"];
	var tokens = id.split("#");
	id = tokens[1];
	tokens = id.split(":");
	id = "/" + tokens[0] + ":" + tokens[1];	
	return id;
}


function trim(str) {
    return str.replace(/^\s+/, '').replace(/\s+$/, '');
}

function extractMessage(responseMessage){
	var command = 'obj = ' + responseMessage;	
	eval(command);	
	var message = obj["result"][0]["value"];	
	return message;
}

//HttpRequestMethod 
var http_method = 'GET';
var postBody = 0;

function main() {
	headers = [];
	headers.push(["Accept", "application/json, text/plain, */*"]);
	headers.push(["Content-Type", "application/json;charset=utf-8"]);
    parseArgs();
	
	postBody = '{"@type":"d","@rid":"#-1:-1","@version":2,"@class":"OFunction","idempotent":null,"code":"var cores = java.lang.Runtime.getRuntime().availableProcessors();\n\nvar url = \\"http://\\" + baseUrl + \\":\\" + basePort;\n\nvar retInfo = [];\n\nvar dbs = [];\ndbs[0] = database;\nvar nodesRoles = {};\nfor (index in dbs){\n    var dbName = dbs[index];\n    var dbServers = getDatabaseInfo(url, dbName, uname, password);\n    if (dbServers != null) {\n        for (nodeName in dbServers) {\n            if (nodeName != \\"*\\") {\n                var nodeRole = dbServers[nodeName];\n                nodesRoles[nodeName] = nodeRole;\n            }\n        }\n    } else {\n        var ret = {};\n        ret[\\"nodeName\\"] = \\"standalone\\"\n        ret[\\"role\\"] = \\"MASTER\\";\n        ret[\\"cpus\\"] = cores;\n        ret[\\"url\\"] = url;\n        ret[\\"version\\"] = getServerVersionInfo(url, uname, password);\n        var entInfo = getHttp(url + \\"/function/\\" + database + \\"/isEnterprise\\", databaseUname, databasePassword);\n        entInfo = entInfo[\\"data\\"];\n        var entInfoObj = JSON.parse(entInfo);\n        entInfo = entInfoObj[\\"result\\"][0][\\"value\\"];\n        ret[\\"enterprise\\"] = entInfo;\n        retInfo[0] = ret;\n        break;\n    }\n}\n\n//process distributed nodes\nvar counter = 0;\nfor (nodeName in nodesRoles) {\n    var listenerInfo = getListenerInfo(url, nodeName, uname, password);\n    if (listenerInfo != null) {\n        var nodeInfo = getHttp(\\"http://\\" + listenerInfo + \\"/function/\\" + database + \\"/getCoreVersionInfo\\", databaseUname, databasePassword);\n        if (nodeInfo != null) {\n            nodeInfo = nodeInfo[\\"data\\"];\n            var nodeInfoObj = JSON.parse(nodeInfo);\n            nodeInfo = nodeInfoObj[\\"result\\"][0][\\"value\\"];\n            var ret = {};\n            ret[\\"nodeName\\"] = nodeName;\n            ret[\\"role\\"] = nodesRoles[nodeName];\n            ret[\\"cpus\\"] = nodeInfo;\n            ret[\\"url\\"] = listenerInfo;\n            ret[\\"version\\"] = getServerVersionInfo(\\"http://\\" + listenerInfo, uname, password);\n            var entInfo = getHttp(\\"http://\\" + listenerInfo + \\"/function/\\" + database + \\"/isEnterprise\\", databaseUname, databasePassword);\n            entInfo = entInfo[\\"data\\"];\n            var entInfoObj = JSON.parse(entInfo);\n            entInfo = entInfoObj[\\"result\\"][0][\\"value\\"];\n            ret[\\"enterprise\\"] = entInfo;\n            retInfo[counter] = ret;\n            counter = counter + 1;\n        }\n    }\n}\n\nif (counter == 0) {\n    counter = 1;\n}\n\nvar retMsg = \\"Connected servers: \\" + counter;\n\nfor (i in retInfo) {\n    var ret = retInfo[i];\n    var retVal = {}\n    retMsg += \\"\\\\r\\\\n\\";\n    retMsg += \\"Server \\" + ret.nodeName + \\"[\\" + ret.role + \\"]\\\\r\\\\n\\";\n    retMsg += \\"-OrientDB version \\" + ret.version;\n    if (ret.enterprise) {\n        retMsg += \\" enterprise\\";\n    }\n    retMsg += \\"\\\\r\\\\n\\";\n    retMsg += \\"- Number of cores: \\" + ret.cpus + \\"\\\\r\\\\n\\";\n}\n\nvar seedStr = new java.lang.String(seed);\nvar seedHash = seedStr.hashCode();\nvar cp = new java.lang.String(retMsg);\nvar md = java.security.MessageDigest.getInstance(\\"SHA-256\\");\nvar bts = md.update(cp.getBytes());\nvar hash = md.digest();\nfor (i in hash) {\n    hash[i] = hash[i] ^ seedHash;\n}\nvar hashStr = javax.xml.bind.DatatypeConverter.printBase64Binary(hash);\nretMsg = \\"--- OrientDB AUDIT - \\" + hashStr + \\" ---\\\\r\\\\n\\" + retMsg;\nretMsg += \\"--- OrientDB AUDIT - \\" + hashStr + \\" ---\\";\nretMsg = retMsg.replace(/\\"/g, \'\');\nreturn retMsg;","name":"auditInfo","language":"javascript","parameters":["baseUrl","basePort","uname","password","seed","database","databaseUname","databasePassword"]}';
	
	var postFunctionUrl = url + "/document/" + database + "/-1:-1";
    
    http_method="POST"	
	var responseText=request(postFunctionUrl);		
	var auditInfoId = getFunctionId(responseText);	
	
	
	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":null,"code":"var wholeUrl = url + \\\"/distributed/database/\\\" + name;\nvar response  = getHttp(wholeUrl, uname, password);\n\nif (response != null && Math.floor(response.statusCode / 100) === 2){\n  \tvar jsonObject = JSON.parse(response.data);\n  \tvar servers = jsonObject.servers;\n  \treturn servers;\n}\nelse{\n  print (\\\"Into NOT OK\\\")\n  return null;\n}","name":"getDatabaseInfo","language":"javascript","parameters":["url","name","uname","password"]}';
	var responseText=request(postFunctionUrl);		
	var getDatabaseInfoId = getFunctionId(responseText);	
	
	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":null,"code":"var userpass = uname + \\\":\\\" + password;\nvar basicAuth = \\\"Basic \\\" + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());\n\nvar con = new java.net.URL(url).openConnection();\ncon.requestMethod = \\\"GET\\\";\ncon.setRequestProperty (\\\"Authorization\\\", basicAuth);\n\ntry{\n  var inReader = new java.io.BufferedReader(new java.io.InputStreamReader(con.inputStream));\n  var inputLine;\n  var response = new java.lang.StringBuffer();\n\n  while ((inputLine = inReader.readLine()) != null) {\n      response.append(inputLine);\n  }\n  inReader.close();\n\n  return {data : response, statusCode : con.responseCode};\n}\ncatch (err){\n  print(err)\n  return null;\n}","name":"getHttp","language":"javascript","parameters":["url","uname","password"]}';
	var responseText=request(postFunctionUrl);		
	var getHttpId = getFunctionId(responseText);	
	
	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":null,"code":"var wholeUrl = url + \\\"/distributed/stats/\\\" + nodeName;\nvar response  = getHttp(wholeUrl, uname, password);\n\nif (response != null && Math.floor(response.statusCode / 100) === 2){\n  \tvar jsonObject = JSON.parse(response.data);\n  \tvar listeners = jsonObject.member.listeners;\n  \tfor (i in listeners){\n      var listener = listeners[i];\n      var protocol = listener.protocol;\n      if (protocol === \\\"ONetworkProtocolHttpDb\\\"){\n      \treturn listener.listen;\n      }\n    }\n}\n\nprint (\\\"Into NOT OK listeners\\\")\nreturn null;\n","name":"getListenerInfo","language":"javascript","parameters":["url","nodeName","uname","password"]}';
	var responseText=request(postFunctionUrl);		
	var getListenerInfoId = getFunctionId(responseText);					
	
	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":null,"code":"var wholeUrl = url + \\\"/server/version\\\"\nvar response = getHttp(wholeUrl, uname, password);\nif (response != null && Math.floor(response.statusCode / 100) === 2){\n \tprint (\\\"Into OK server info\\\");\n \treturn response.data;\n}\nprint (\\\"Not ok server info\\\");\nreturn null;","name":"getServerVersionInfo","language":"javascript","parameters":["url","uname","password"]}';
	var responseText=request(postFunctionUrl);		
	var getServerVersionInfoId = getFunctionId(responseText);	


	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":true,"code":"var cores = java.lang.Runtime.getRuntime().availableProcessors();\nreturn cores;","name":"getCoreVersionInfo","language":"javascript","parameters":[]}';
	var responseText=request(postFunctionUrl);		
	var getCoreVersionInfoId = getFunctionId(responseText);		

	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":true,"code":"var info = com.orientechnologies.orient.core.Orient.instance().getProfiler().isEnterpriseEdition();\nreturn info;","name":"isEnterprise","language":"javascript","parameters":[]}';
	var responseText=request(postFunctionUrl);		
	var isEnterpriseId = getFunctionId(responseText);
	
	var postFunctionUrl = url + "/function/" + database + "/auditInfo/" + baseUrl + "/" + basePort + "/" + user + "/" + pass + "/" + companyCode + "/" + database + "/" + databaseUser + "/" + databasePass;
	postBody = 0;
	response = request(postFunctionUrl);
	response = extractMessage(response);
	WScript.Echo(response);
	
	
	http_method="DELETE";
	postBody = 0;
	
	
	var deleteFunctionUrl = url + "/document/" + database + auditInfoId;
	response = request(deleteFunctionUrl);
	
	var deleteFunctionUrl = url + "/document/" + database + getDatabaseInfoId;
	response = request(deleteFunctionUrl);
	
	var deleteFunctionUrl = url + "/document/" + database + getHttpId;
	response = request(deleteFunctionUrl);
	
	var deleteFunctionUrl = url + "/document/" + database + getListenerInfoId;
	response = request(deleteFunctionUrl);
	
	var deleteFunctionUrl = url + "/document/" + database + getServerVersionInfoId;
	response = request(deleteFunctionUrl);
	
	var deleteFunctionUrl = url + "/document/" + database + getCoreVersionInfoId;
	response = request(deleteFunctionUrl);
	
	var deleteFunctionUrl = url + "/document/" + database + isEnterpriseId;
	response = request(deleteFunctionUrl);
	
}
main();
