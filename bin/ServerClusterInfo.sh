#!/bin/bash


uname=$3
password=$4
database=$5
baseUrl=$1
basePort=$2
databaseUser=$6
databasePassword=$7
companyCode=$8

url="http://${baseUrl}:${basePort}"
#echo "${url}"

#create functions

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json;charset=utf-8' --data '{\"@class\":\"ofunction\",\"@version\":0,\"@rid\":\"#-1:-1\",\"idempotent\":null,\"name\":\"auditInfo\",\"language\":\"javascript\",\"code\":\"var cores = java.lang.Runtime.getRuntime().availableProcessors();\\n\\nvar url = \\\"http://\\\" + baseUrl + \\\":\\\" + basePort;\\n\\nvar retInfo = [];\\n\\nvar nodesInfo = getNodesNamesAndIPs();\\n\\n//process distributed nodes\\nvar counter = 0;\\nfor (var i in nodesInfo) {\\n  \\tvar nodeInfo = nodesInfo[i];\\n    var listenerInfo = nodeInfo.ip;    \\n    var cpuInfo = getHttp(\\\"http://\\\" + listenerInfo + \\\"/function/\\\" + database + \\\"/getCoreVersionInfo\\\", databaseUname, databasePassword);\\n    if (cpuInfo != null) {\\n      cpuInfo = cpuInfo[\\\"data\\\"];\\n      var cpuInfoObj = JSON.parse(cpuInfo);\\n      cpuInfo = cpuInfoObj[\\\"result\\\"][0][\\\"value\\\"];\\n      var ret = {};\\n      ret[\\\"nodeName\\\"] = nodeInfo.name;\\n      ret[\\\"role\\\"] = nodeInfo.role;\\n      ret[\\\"cpus\\\"] = cpuInfo;\\n      ret[\\\"url\\\"] = listenerInfo;\\n      ret[\\\"version\\\"] = getServerVersionInfo(\\\"http://\\\" + listenerInfo, uname, password);\\n      var entInfo = getHttp(\\\"http://\\\" + listenerInfo + \\\"/function/\\\" + database + \\\"/isEnterprise\\\", databaseUname, databasePassword);\\n      entInfo = entInfo[\\\"data\\\"];\\n      var entInfoObj = JSON.parse(entInfo);\\n      entInfo = entInfoObj[\\\"result\\\"][0][\\\"value\\\"];\\n      ret[\\\"enterprise\\\"] = entInfo;\\n      retInfo[counter] = ret;\\n      counter = counter + 1;\\n    }    \\n}\\n\\nvar retMsg = \\\"Connected servers: \\\" + counter;\\n\\nfor (i in retInfo) {\\n    var ret = retInfo[i];\\n    var retVal = {}\\n    retMsg += \\\"\\\\n\\\";\\n    retMsg += \\\"Server \\\" + ret.nodeName + \\\"[\\\" + ret.role + \\\"]\\\\n\\\";\\n    retMsg += \\\"-OrientDB version \\\" + ret.version;\\n    if (ret.enterprise) {\\n        retMsg += \\\" enterprise\\\";\\n    }\\n    retMsg += \\\"\\\\n\\\";\\n    retMsg += \\\"- Number of cores: \\\" + ret.cpus + \\\"\\\\n\\\";\\n}\\n\\nvar currentTime = java.lang.System.currentTimeMillis() / 10000;\\nvar rnd = new java.util.Random();\\nvar rndNum = java.lang.Integer.valueOf(rnd.nextInt(1000000)).longValue();\\ncurrentTime |= rndNum;\\nprint(currentTime);\\nvar seedStr = new java.lang.String(seed + currentTime);\\nvar seedHash = seedStr.hashCode();\\nvar cp = new java.lang.String(retMsg);\\nvar md = java.security.MessageDigest.getInstance(\\\"SHA-256\\\");\\nvar bts = md.update(cp.getBytes());\\nvar hash = md.digest();\\nfor (i in hash) {\\n    hash[i] = hash[i] ^ seedHash;\\n}\\nvar hashStr = javax.xml.bind.DatatypeConverter.printBase64Binary(hash);\\nretMsg = \\\"--- OrientDB AUDIT - \\\" + hashStr + currentTime + \\\" ---\\\\n\\\" + retMsg;\\nretMsg += \\\"--- OrientDB AUDIT - \\\" + hashStr + currentTime + \\\" ---\\\";\\nretMsg = retMsg.replace(/\\\"/g, \\\"\\\");\\nreturn retMsg;\",\"parameters\":[\"baseUrl\",\"basePort\",\"uname\",\"password\",\"seed\",\"database\",\"databaseUname\",\"databasePassword\"]}'"
#echo "${command}"

autidInfoID="$(eval ${command} | grep -Po '"@rid":".*?[^\\]"' | awk 'BEGIN{RS="#";FS=":";} (NR==2) {print "/" $1 ":" $2}' | awk -F'"' '{print $1}')"
#echo "${autidInfoID}"

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json;charset=utf-8' --data '{\"@type\":\"d\",\"@rid\":\"#-1:-1\",\"@version\":0,\"@class\":\"OFunction\",\"idempotent\":null,\"code\":\"var wholeUrl = url + \\\"/distributed/database/\\\" + name;\\nprint (wholeUrl);\\nvar response  = getHttp(wholeUrl, uname, password);\\n\\nif (response != null && Math.floor(response.statusCode / 100) === 2){\\n  \\tprint (\\\"Into OK\\\")\\n \\tvar jsonObject = JSON.parse(response.data);\\n  \\tvar servers = jsonObject.servers;\\n  \\treturn servers;\\n}\\nelse{\\n  print (\\\"Into NOT OK\\\")\\n  return null;\\n}\",\"name\":\"getDatabaseInfo\",\"language\":\"javascript\",\"parameters\":[\"url\",\"name\",\"uname\",\"password\"]}'"

getDatabaseInfoID="$(eval ${command} | grep -Po '"@rid":".*?[^\\]"' | awk 'BEGIN{RS="#";FS=":";} (NR==2) {print "/" $1 ":" $2}' | awk -F'"' '{print $1}')"
#echo "${getDatabaseInfoID}"

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json;charset=utf-8' --data '{\"@type\":\"d\",\"@rid\":\"#-1:-1\",\"@version\":0,\"@class\":\"OFunction\",\"idempotent\":null,\"code\":\"var userpass = uname + \\\":\\\" + password;\\nvar basicAuth = \\\"Basic \\\" + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());\\n\\nvar con = new java.net.URL(url).openConnection();\\ncon.requestMethod = \\\"GET\\\";\\ncon.setRequestProperty (\\\"Authorization\\\", basicAuth);\\n\\ntry{\\n  var inReader = new java.io.BufferedReader(new java.io.InputStreamReader(con.inputStream));\\n  var inputLine;\\n  var response = new java.lang.StringBuffer();\\n\\n  while ((inputLine = inReader.readLine()) != null) {\\n      response.append(inputLine);\\n  }\\n  inReader.close();\\n\\n  return {data : response, statusCode : con.responseCode};\\n}\\ncatch (err){\\n  print(err)\\n  return null;\\n}\",\"name\":\"getHttp\",\"language\":\"javascript\",\"parameters\":[\"url\",\"uname\",\"password\"]}'"

getHttpID="$(eval ${command} | grep -Po '"@rid":".*?[^\\]"' | awk 'BEGIN{RS="#";FS=":";} (NR==2) {print "/" $1 ":" $2}' | awk -F'"' '{print $1}')"
#echo "${getHttpID}"

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json;charset=utf-8' --data '{\"@type\":\"d\",\"@rid\":\"#-1:-1\",\"@version\":0,\"@class\":\"OFunction\",\"idempotent\":null,\"code\":\"var wholeUrl = url + \\\"/distributed/stats/\\\" + nodeName;\\nvar response  = getHttp(wholeUrl, uname, password);\\n\\nif (response != null && Math.floor(response.statusCode / 100) === 2){\\n  \\tprint (\\\"Into OK listeners\\\")\\n \\tvar jsonObject = JSON.parse(response.data);\\n  \\tvar listeners = jsonObject.member.listeners;\\n  \\tfor (i in listeners){\\n      var listener = listeners[i];\\n      var protocol = listener.protocol;\\n      if (protocol === \\\"ONetworkProtocolHttpDb\\\"){\\n      \\treturn listener.listen;\\n      }\\n    }\\n}\\n\\nprint (\\\"Into NOT OK listeners\\\")\\nreturn null;\\n\",\"name\":\"getListenerInfo\",\"language\":\"javascript\",\"parameters\":[\"url\",\"nodeName\",\"uname\",\"password\"]}'"

getListenerInfoID="$(eval ${command} | grep -Po '"@rid":".*?[^\\]"' | awk 'BEGIN{RS="#";FS=":";} (NR==2) {print "/" $1 ":" $2}' | awk -F'"' '{print $1}')"
#echo "${getListenerInfoID}"

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Content-Type: application/json;charset=UTF-8' -H 'Accept: application/json, text/plain, */*' --data '{\"@type\":\"d\",\"@rid\":\"#-1:-1\",\"@version\":0,\"@class\":\"OFunction\",\"idempotent\":null,\"code\":\"var wholeUrl = url + \\\"/server/version\\\"\\nvar response = getHttp(wholeUrl, uname, password);\\nif (response != null && Math.floor(response.statusCode / 100) === 2){\\n \\tprint (\\\"Into OK server info\\\");\\n \\treturn response.data;\\n}\\nprint (\\\"Not ok server info\\\");\\nreturn null;\",\"name\":\"getServerVersionInfo\",\"language\":\"javascript\",\"parameters\":[\"url\",\"uname\",\"password\"]}'"
#echo "${command}"

getServerVersionInfoID="$(eval ${command} | grep -Po '"@rid":".*?[^\\]"' | awk 'BEGIN{RS="#";FS=":";} (NR==2) {print "/" $1 ":" $2}' | awk -F'"' '{print $1}')"
#echo "${getServerVersionInfoID}"

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json;charset=utf-8' --data '{\"@type\":\"d\",\"@rid\":\"#-1:-1\",\"@version\":0,\"@class\":\"OFunction\",\"idempotent\":true,\"code\":\"var cores = java.lang.Runtime.getRuntime().availableProcessors();\\nreturn cores;\",\"name\":\"getCoreVersionInfo\",\"language\":\"javascript\",\"parameters\":[]}'"

getCoreVersionInfoID="$(eval ${command} | grep -Po '"@rid":".*?[^\\]"' | awk 'BEGIN{RS="#";FS=":";} (NR==2) {print "/" $1 ":" $2}' | awk -F'"' '{print $1}')"
#echo "${getCoreVersionInfoID}"

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json;charset=utf-8' --data '{\"@type\":\"d\",\"@rid\":\"#-1:-1\",\"@version\":0,\"@class\":\"OFunction\",\"idempotent\":true,\"code\":\"var info = com.orientechnologies.orient.core.Orient.instance().getProfiler().isEnterpriseEdition();\\nreturn info;\",\"name\":\"isEnterprise\",\"language\":\"javascript\",\"parameters\":[]}'"

isEnterpriseID="$(eval ${command} | grep -Po '"@rid":".*?[^\\]"' | awk 'BEGIN{RS="#";FS=":";} (NR==2) {print "/" $1 ":" $2}' | awk -F'"' '{print $1}')"
#echo "${isEnterpriseID}"

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json;charset=utf-8' --data '{\"@type\":\"d\",\"@rid\":\"#6:104\",\"@version\":11,\"@class\":\"OFunction\",\"idempotent\":null,\"code\":\"var haResponse = orient.getDatabase().command(\\\"HA STATUS -servers -output=text\\\");    \\nif (Object.prototype.toString.call(haResponse).startsWith(\\\"[object [L\\\")) {  \\n  haResponse = haResponse[0];\\n  haResponse = haResponse.field(\\\"value\\\");\\n}\\nvar lines = haResponse.split(\\\"\\\n\\\");\\nvar retArr = [];\\nvar header = true;\\nfor (var i  in lines) {\\n  var line = lines[i];\\n  if (line.indexOf(\\\"|\\\") !== -1) {    \\n    if (header) {\\n      header = false;\\n    } else {\\n      var tokens = line.split(\\\"|\\\");\\n      if (tokens[1].trim() !== \\\"\\\"){\\n        var retObj = new Object();\\n        retObj.ip = tokens[7];\\n        var name = tokens[1];\\n        var openBracketIndex = name.indexOf(\\\"(\\\");\\n        if (openBracketIndex !== -1) {\\n          name = name.substring(0, openBracketIndex);\\n        }\\n        name = name.trim();\\n        retObj.name = name;\\n        var role = tokens[3];\\n        openBracketIndex = role.lastIndexOf(\\\"(\\\");\\n        var closedBracketIndex = role.lastIndexOf(\\\")\\\");\\n        if (openBracketIndex !== -1 && closedBracketIndex !== -1 && closedBracketIndex > openBracketIndex) {\\n          role = role.substring(openBracketIndex + 1, closedBracketIndex);\\n        } else {\\n          role = \\\"unknown\\\";\\n        }\\n\\n        retObj.role = role;\\n        retArr.push(retObj);\\n      }\\n    }\\n  }\\n}\\nreturn retArr;\",\"name\":\"getNodesNamesAndIPs\",\"language\":\"javascript\",\"parameters\":null}'"

getNodesNamesAndIPsID="$(eval ${command} | grep -Po '"@rid":".*?[^\\]"' | awk 'BEGIN{RS="#";FS=":";} (NR==2) {print "/" $1 ":" $2}' | awk -F'"' '{print $1}')"

#call functions

command="curl -s -u ${uname}:${password} -X POST '${url}/function/${database}/auditInfo/${baseUrl}/${basePort}/${uname}/${password}/${companyCode}/${database}/${databaseUser}/${databasePassword}'"
#echo "${command}"
res="$(eval ${command} | grep -Po '"value":".*?[^\\]"' | awk -F'"value":' '{print $2}')"
echo -e ${res}

#delete functions


command="curl -s -u ${uname}:${password} 'http://localhost:2480/document/${database}${autidInfoID}' -X DELETE -H 'Accept: application/json, text/plain, */*'"
res="$(eval ${command})"

command="curl -s -u ${uname}:${password} 'http://localhost:2480/document/${database}${getDatabaseInfoID}' -X DELETE -H 'Accept: application/json, text/plain, */*'"
res="$(eval ${command})"

command="curl -s -u ${uname}:${password} 'http://localhost:2480/document/${database}${getHttpID}' -X DELETE -H 'Accept: application/json, text/plain, */*'"
res="$(eval ${command})"

command="curl -s -u ${uname}:${password} 'http://localhost:2480/document/${database}${getListenerInfoID}' -X DELETE -H 'Accept: application/json, text/plain, */*'"
res="$(eval ${command})"


command="curl -s -u ${uname}:${password} 'http://localhost:2480/document/${database}${getServerVersionInfoID}' -X DELETE -H 'Accept: application/json, text/plain, */*'"
res="$(eval ${command})"

command="curl -s -u ${uname}:${password} 'http://localhost:2480/document/${database}${getCoreVersionInfoID}' -X DELETE -H 'Accept: application/json, text/plain, */*'"
res="$(eval ${command})"

command="curl -s -u ${uname}:${password} 'http://localhost:2480/document/${database}${isEnterpriseID}' -X DELETE -H 'Accept: application/json, text/plain, */*'"
res="$(eval ${command})"

command="curl -s -u ${uname}:${password} 'http://localhost:2480/document/${database}${getNodesNamesAndIPsID}' -X DELETE -H 'Accept: application/json, text/plain, */*'"
res="$(eval ${command})"
