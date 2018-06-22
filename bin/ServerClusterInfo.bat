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
		WScript.Echo(functionUrl);
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
	
	postBody = '{"@type":"d","@rid":"#-1:-1","@version":2,"@class":"OFunction","idempotent":null,"code":"var cores = java.lang.Runtime.getRuntime().availableProcessors();\n\nvar url = \\\"http://\\" + baseUrl + \\":\\" + basePort;\n\nvar retInfo = [];\n\nvar nodesInfo = getNodesNamesAndIPs();\n\n//process distributed nodes\nvar counter = 0;\nfor (var i in nodesInfo) {\n  \\tvar nodeInfo = nodesInfo[i];\n    var listenerInfo = nodeInfo.ip;    \n    var cpuInfo = getHttp(\\"http://\\" + listenerInfo + \\"/function/\\" + database + \\"/getCoreVersionInfo\\", databaseUname, databasePassword);\n    if (cpuInfo != null) {\n      cpuInfo = cpuInfo[\\"data\\"];\n      var cpuInfoObj = JSON.parse(cpuInfo);\n      cpuInfo = cpuInfoObj[\\"result\\"][0][\\"value\\"];\n      var ret = {};\n      ret[\\"nodeName\\"] = nodeInfo.name;\n      ret[\\"role\\"] = nodeInfo.role;\n      ret[\\"cpus\\"] = cpuInfo;\n      ret[\\"url\\"] = listenerInfo;\n      ret[\\"version\\"] = getServerVersionInfo(\\"http://\\" + listenerInfo, uname, password);\n      var entInfo = getHttp(\\"http://\\" + listenerInfo + \\"/function/\\" + database + \\"/isEnterprise\\", databaseUname, databasePassword);\n      entInfo = entInfo[\\"data\\"];\n      var entInfoObj = JSON.parse(entInfo);\n      entInfo = entInfoObj[\\"result\\"][0][\\"value\\"];\n      ret[\\"enterprise\\"] = entInfo;\n      retInfo[counter] = ret;\n      counter = counter + 1;\n    }    \n}\n\nvar retMsg = \\"Connected servers: \\" + counter;\n\nfor (i in retInfo) {\n    var ret = retInfo[i];\n    var retVal = {}\n    retMsg += \\"\\\\r\\\\n\\";\n    retMsg += \\"Server \\" + ret.nodeName + \\"[\\" + ret.role + \\"]\\\\r\\\\n\\";\n    retMsg += \\"-OrientDB version \\" + ret.version;\n    if (ret.enterprise) {\n        retMsg += \\" enterprise\\";\n    }\n    retMsg += \\"\\\\r\\\\n\\";\n    retMsg += \\"- Number of cores: \\" + ret.cpus + \\"\\\\r\\\\n\\";\n}\n\nvar currentTime = java.lang.System.currentTimeMillis() / 10000;\nvar rnd = new java.util.Random();\nvar rndNum = java.lang.Integer.valueOf(rnd.nextInt(1000000)).longValue();\ncurrentTime |= rndNum;\nprint(currentTime);\nvar seedStr = new java.lang.String(seed + currentTime);\nvar seedHash = seedStr.hashCode();\nvar cp = new java.lang.String(retMsg);\nvar md = java.security.MessageDigest.getInstance(\\"SHA-256\\");\nvar bts = md.update(cp.getBytes());\nvar hash = md.digest();\nfor (i in hash) {\n    hash[i] = hash[i] ^ seedHash;\n}\nvar hashStr = javax.xml.bind.DatatypeConverter.printBase64Binary(hash);\nretMsg = \\"--- OrientDB AUDIT - \\" + hashStr + currentTime + \\" ---\\\\r\\\\n\\" + retMsg;\nretMsg += \\"--- OrientDB AUDIT - \\" + hashStr + currentTime + \\" ---\\";\nretMsg = retMsg.replace(/\\"/g, \'\');\nreturn retMsg;","name":"auditInfo","language":"javascript","parameters":["baseUrl","basePort","uname","password","seed","database","databaseUname","databasePassword"]}';
	
	var postFunctionUrl = url + "/document/" + database + "/-1:-1";
    
    http_method="POST"	
	var responseText=request(postFunctionUrl);
	//WScript.Echo(responseText);
	var auditInfoId = getFunctionId(responseText);	
	//WScript.Echo(auditInfoId);
	
	
	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":null,"code":"var wholeUrl = url + \\\"/distributed/database/\\\" + name;\nvar response  = getHttp(wholeUrl, uname, password);\n\nif (response != null && Math.floor(response.statusCode / 100) === 2){\n  \tvar jsonObject = JSON.parse(response.data);\n  \tvar servers = jsonObject.servers;\n  \treturn servers;\n}\nelse{\n  print (\\\"Into NOT OK\\\")\n  return null;\n}","name":"getDatabaseInfo","language":"javascript","parameters":["url","name","uname","password"]}';
	var responseText=request(postFunctionUrl);		
	var getDatabaseInfoId = getFunctionId(responseText);	
	//WScript.Echo(getDatabaseInfoId);
	
	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":null,"code":"var userpass = uname + \\\":\\\" + password;\nvar basicAuth = \\\"Basic \\\" + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());\n\nvar con = new java.net.URL(url).openConnection();\ncon.requestMethod = \\\"GET\\\";\ncon.setRequestProperty (\\\"Authorization\\\", basicAuth);\n\ntry{\n  var inReader = new java.io.BufferedReader(new java.io.InputStreamReader(con.inputStream));\n  var inputLine;\n  var response = new java.lang.StringBuffer();\n\n  while ((inputLine = inReader.readLine()) != null) {\n      response.append(inputLine);\n  }\n  inReader.close();\n\n  return {data : response, statusCode : con.responseCode};\n}\ncatch (err){\n  print(err)\n  return null;\n}","name":"getHttp","language":"javascript","parameters":["url","uname","password"]}';
	var responseText=request(postFunctionUrl);		
	var getHttpId = getFunctionId(responseText);	
	//WScript.Echo(getHttpId);
	
	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":null,"code":"var wholeUrl = url + \\\"/distributed/stats/\\\" + nodeName;\nvar response  = getHttp(wholeUrl, uname, password);\n\nif (response != null && Math.floor(response.statusCode / 100) === 2){\n  \tvar jsonObject = JSON.parse(response.data);\n  \tvar listeners = jsonObject.member.listeners;\n  \tfor (i in listeners){\n      var listener = listeners[i];\n      var protocol = listener.protocol;\n      if (protocol === \\\"ONetworkProtocolHttpDb\\\"){\n      \treturn listener.listen;\n      }\n    }\n}\n\nprint (\\\"Into NOT OK listeners\\\")\nreturn null;\n","name":"getListenerInfo","language":"javascript","parameters":["url","nodeName","uname","password"]}';
	var responseText=request(postFunctionUrl);		
	var getListenerInfoId = getFunctionId(responseText);					
	//WScript.Echo(getListenerInfoId);
	
	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":null,"code":"var wholeUrl = url + \\\"/server/version\\\"\nvar response = getHttp(wholeUrl, uname, password);\nif (response != null && Math.floor(response.statusCode / 100) === 2){\n \tprint (\\\"Into OK server info\\\");\n \treturn response.data;\n}\nprint (\\\"Not ok server info\\\");\nreturn null;","name":"getServerVersionInfo","language":"javascript","parameters":["url","uname","password"]}';
	var responseText=request(postFunctionUrl);		
	var getServerVersionInfoId = getFunctionId(responseText);	
	//WScript.Echo(getServerVersionInfoId);
	

	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":true,"code":"var cores = java.lang.Runtime.getRuntime().availableProcessors();\nreturn cores;","name":"getCoreVersionInfo","language":"javascript","parameters":[]}';
	var responseText=request(postFunctionUrl);		
	var getCoreVersionInfoId = getFunctionId(responseText);		
	//WScript.Echo(getCoreVersionInfoId);

	
	postBody='{"@type":"d","@rid":"#-1:-1","@version":0,"@class":"OFunction","idempotent":true,"code":"var info = com.orientechnologies.orient.core.Orient.instance().getProfiler().isEnterpriseEdition();\nreturn info;","name":"isEnterprise","language":"javascript","parameters":[]}';
	var responseText=request(postFunctionUrl);		
	var isEnterpriseId = getFunctionId(responseText);
	//WScript.Echo(isEnterpriseId);
	
	postBody='{"@type":"d","@rid":"#6:23","@version":12,"@class":"OFunction","idempotent":null,"code":"var haResponse = orient.getDatabase().command(\\\"HA STATUS -servers -output=text\\\");\nhaResponse = haResponse[0];\nhaResponse = haResponse.field(\\\"value\\\");\nvar lines = haResponse.split(\\\"\\\\n\\\");\nvar retArr = [];\nvar header = true;\nfor (var i in lines){\n  var line = lines[i];\n  if (line.indexOf(\\\"|\\\") !== -1){\n    if (header){\n      header = false;\n    }\n    else{\n      var tokens = line.split(\\\"|\\\");\n      var retObj = new Object();\n      retObj.ip = tokens[7];\n      var name = tokens[1];\n      var openBracketIndex = name.indexOf(\\\"(\\\");\n      if (openBracketIndex !== -1){\n        name = name.substring(0, openBracketIndex);\n      }\n      name = name.trim();\n      retObj.name = name;\n      var role = tokens[3];\n      openBracketIndex = role.lastIndexOf(\\\"(\\\");\n      var closedBracketIndex = role.lastIndexOf(\\\")\\\");\n      if (openBracketIndex !== -1 && closedBracketIndex !== -1 && closedBracketIndex > openBracketIndex){\n        role = role.substring(openBracketIndex + 1, closedBracketIndex);\n      }\n      else{\n        role = \\\"unknown\\\";\n      }\n      \n      retObj.role = role;\n      print(retObj.name + \\\", \\\" + retObj.role + \\\", \\\" + retObj.ip);\n  retArr.push(retObj);\n    }\n  }\n}\nreturn retArr;","name":"getNodesNamesAndIPs","language":"javascript","parameters":null}';
	var responseText=request(postFunctionUrl);		
	var getNodesAndIpsID = getFunctionId(responseText);
	//WScript.Echo(getNodesAndIpsID);
	
	var postFunctionUrl = url + "/function/" + database + "/auditInfo/" + baseUrl + "/" + basePort + "/" + user + "/" + pass + "/" + companyCode + "/" + database + "/" + databaseUser + "/" + databasePass;
	postBody = 0;
	response = request(postFunctionUrl);
	//WScript.Echo(response);
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
	
	var deleteFunctionUrl = url + "/document/" + database + getNodesAndIpsID;
	response = request(deleteFunctionUrl);
	
}
main();
