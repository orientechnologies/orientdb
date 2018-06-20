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

command="curl -s -u ${uname}:${password} '${url}/document/${database}/-1:-1' -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json;charset=utf-8' --data $'{\"@type\":\"d\",\"@rid\":\"#-1:-1\",\"@version\":2,\"@class\":\"OFunction\",\"idempotent\":null,\"code\":\"var cores = java.lang.Runtime.getRuntime().availableProcessors();\\\\nprint(cores)\\\\n\\\\nvar url = \\\\\"http://\\\\\" + baseUrl + \\\\\":\\\\\" + basePort;\\\\n\\\\nvar retInfo = [];\\\\n\\\\nvar dbs = [];\\\\ndbs[0] = database;\\\\nvar nodesRoles = {};\\\\nfor (index in dbs){\\\\n    var dbName = dbs[index];\\\\n    var dbServers = getDatabaseInfo(url, dbName, uname, password);\\\\n    if (dbServers != null) {\\\\n        for (nodeName in dbServers) {\\\\n            if (nodeName != \\\\\"*\\\\\") {\\\\n                var nodeRole = dbServers[nodeName];\\\\n                nodesRoles[nodeName] = nodeRole;\\\\n            }\\\\n        }\\\\n    } else {\\\\n        var ret = {};\\\\n        ret[\\\\\"nodeName\\\\\"] = \\\\\"standalone\\\\\"\\\\n        ret[\\\\\"role\\\\\"] = \\\\\"MASTER\\\\\";\\\\n        ret[\\\\\"cpus\\\\\"] = cores;\\\\n        ret[\\\\\"url\\\\\"] = url;\\\\n        ret[\\\\\"version\\\\\"] = getServerVersionInfo(url, uname, password);\\\\n        var entInfo = getHttp(url + \\\\\"/function/\\\\\" + database + \\\\\"/isEnterprise\\\\\", databaseUname, databasePassword);\\\\n        entInfo = entInfo[\\\\\"data\\\\\"];\\\\n        var entInfoObj = JSON.parse(entInfo);\\\\n        entInfo = entInfoObj[\\\\\"result\\\\\"][0][\\\\\"value\\\\\"];\\\\n        ret[\\\\\"enterprise\\\\\"] = entInfo;\\\\n        retInfo[0] = ret;\\\\n        break;\\\\n    }\\\\n}\\\\n\\\\n//process distributed nodes\\\\nvar counter = 0;\\\\nfor (nodeName in nodesRoles) {\\\\n    var listenerInfo = getListenerInfo(url, nodeName, uname, password);\\\\n    if (listenerInfo != null) {\\\\n        var nodeInfo = getHttp(\\\\\"http://\\\\\" + listenerInfo + \\\\\"/function/\\\\\" + database + \\\\\"/getCoreVersionInfo\\\\\", databaseUname, databasePassword);\\\\n        if (nodeInfo != null) {\\\\n            nodeInfo = nodeInfo[\\\\\"data\\\\\"];\\\\n            var nodeInfoObj = JSON.parse(nodeInfo);\\\\n            nodeInfo = nodeInfoObj[\\\\\"result\\\\\"][0][\\\\\"value\\\\\"];\\\\n            var ret = {};\\\\n            ret[\\\\\"nodeName\\\\\"] = nodeName;\\\\n            ret[\\\\\"role\\\\\"] = nodesRoles[nodeName];\\\\n            ret[\\\\\"cpus\\\\\"] = nodeInfo;\\\\n            ret[\\\\\"url\\\\\"] = listenerInfo;\\\\n            ret[\\\\\"version\\\\\"] = getServerVersionInfo(\\\\\"http://\\\\\" + listenerInfo, uname, password);\\\\n            var entInfo = getHttp(\\\\\"http://\\\\\" + listenerInfo + \\\\\"/function/\\\\\" + database + \\\\\"/isEnterprise\\\\\", databaseUname, databasePassword);\\\\n            entInfo = entInfo[\\\\\"data\\\\\"];\\\\n            var entInfoObj = JSON.parse(entInfo);\\\\n            entInfo = entInfoObj[\\\\\"result\\\\\"][0][\\\\\"value\\\\\"];\\\\n            ret[\\\\\"enterprise\\\\\"] = entInfo;\\\\n            retInfo[counter] = ret;\\\\n            counter = counter + 1;\\\\n        }\\\\n    }\\\\n}\\\\n\\\\nif (counter == 0) {\\\\n    counter = 1;\\\\n}\\\\n\\\\nvar retMsg = \\\\\"Connected servers: \\\\\" + counter;\\\\n\\\\nfor (i in retInfo) {\\\\n    var ret = retInfo[i];\\\\n    var retVal = {}\\\\n    retMsg += \\\\\"\\\\\\\\n\\\\\";\\\\n    retMsg += \\\\\"Server \\\\\" + ret.nodeName + \\\\\"[\\\\\" + ret.role + \\\\\"]\\\\\\\\n\\\\\";\\\\n    retMsg += \\\\\"-OrientDB version \\\\\" + ret.version;\\\\n    if (ret.enterprise) {\\\\n        retMsg += \\\\\" enterprise\\\\\";\\\\n    }\\\\n    retMsg += \\\\\"\\\\\\\\n\\\\\";\\\\n    retMsg += \\\\\"- Number of cores: \\\\\" + ret.cpus + \\\\\"\\\\\\\\n\\\\\";\\\\n}\\\\n\\\\nvar seedStr = new java.lang.String(seed);\\\\nvar seedHash = seedStr.hashCode();\\\\nvar cp = new java.lang.String(retMsg);\\\\nvar md = java.security.MessageDigest.getInstance(\\\\\"SHA-256\\\\\");\\\\nvar bts = md.update(cp.getBytes());\\\\nvar hash = md.digest();\\\\nfor (i in hash) {\\\\n    hash[i] = hash[i] ^ seedHash;\\\\n}\\\\nvar hashStr = new java.lang.String(hash, \\\\\"UTF-8\\\\\");\\\\nretMsg = \\\\\"--- OrientDB AUDIT - \\\\\" + hashStr + \\\\\" ---\\\\\\\\n\\\\\" + retMsg;\\\\nretMsg += \\\\\"--- OrientDB AUDIT - \\\\\" + hashStr + \\\\\" ---\\\\\";\\\\nretMsg = retMsg.replace(/\\\\\"/g, \\'\\');\\\\nreturn retMsg;\",\"name\":\"auditInfo\",\"language\":\"javascript\",\"parameters\":[\"baseUrl\",\"basePort\",\"uname\",\"password\",\"seed\",\"database\",\"databaseUname\",\"databasePassword\"]}'"
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

