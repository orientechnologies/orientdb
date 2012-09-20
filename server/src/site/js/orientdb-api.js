/*
 * Copyright 1999-2010 Luca Molino
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Represents the main entry-point to work with OrientDB databases.
 * 
 * @author Luca Molino
 */

function ODatabase(databasePath) {
	this.databaseUrl = "";
	this.databaseName = "";
	this.encodedDatabaseName = "";
	this.databaseInfo = null;
	this.commandResult = null;
	this.commandResponse = null;
	this.errorMessage = null;
	this.evalResponse = true;
	this.parseResponseLink = true;
	this.removeObjectCircleReferences = true;
	this.urlPrefix = "/";
	this.urlSuffix = "";

	if (databasePath) {
		var pos = databasePath.indexOf('orientdb_proxy', 8); // JUMP HTTP
		if (pos > -1) {
			pos = databasePath.indexOf('/', pos); // END OF PROXY
		} else {
			pos = databasePath.indexOf('/', 8);
		}

		if (pos > -1) {
			this.databaseUrl = databasePath.substring(0, pos + 1);
			this.databaseName = databasePath.substring(pos + 1);
		} else {
			this.databaseUrl = databasePath;
			this.databaseName = null;
		}

		if (this.databaseName != null && this.databaseName.indexOf('/') > -1) {
			this.encodedDatabaseName = "";
			var parts = this.databaseName.split('/');
			for ( var p in parts) {
				if (!parts.hasOwnProperty(p)) {
					continue;
				}
				if (this.encodedDatabaseName.length > 0)
					this.encodedDatabaseName += '$';
				this.encodedDatabaseName += parts[p];
			}
		} else
			this.encodedDatabaseName = this.databaseName;
	}

	ODatabase.prototype.getDatabaseName = function() {
		return this.databaseName;
	}
	ODatabase.prototype.getDatabaseInfo = function() {
		return this.databaseInfo;
	}
	ODatabase.prototype.setDatabaseInfo = function(iDatabaseInfo) {
		this.databaseInfo = iDatabaseInfo;
	}

	ODatabase.prototype.getUrlSuffix = function() {
		return this.urlSuffix;
	}
	ODatabase.prototype.setUrlSuffix = function(iUrlSuffix) {
		this.urlSuffix = iUrlSuffix;
	}

	ODatabase.prototype.getCommandResult = function() {
		return this.commandResult;
	}
	ODatabase.prototype.setCommandResult = function(iCommandResult) {
		this.commandResult = iCommandResult;
	}

	ODatabase.prototype.getCommandResponse = function() {
		return this.commandResponse;
	}
	ODatabase.prototype.setCommandResponse = function(iCommandResponse) {
		this.commandResponse = iCommandResponse;
	}

	ODatabase.prototype.getErrorMessage = function() {
		return this.errorMessage;
	}
	ODatabase.prototype.setErrorMessage = function(iErrorMessage) {
		this.errorMessage = iErrorMessage;
	}

	ODatabase.prototype.getDatabaseUrl = function() {
		return this.databaseUrl;
	}
	ODatabase.prototype.setDatabaseUrl = function(iDatabaseUrl) {
		this.databaseUrl = iDatabaseUrl;
	}

	ODatabase.prototype.getDatabaseName = function() {
		return this.encodedDatabaseName;
	}
	ODatabase.prototype.setDatabaseName = function(iDatabaseName) {
		this.encodedDatabaseName = iDatabaseName;
	}

	ODatabase.prototype.getEvalResponse = function() {
		return this.evalResponse;
	}
	ODatabase.prototype.setEvalResponse = function(iEvalResponse) {
		this.evalResponse = iEvalResponse;
	}

	ODatabase.prototype.getParseResponseLinks = function() {
		return this.parseResponseLink;
	}
	ODatabase.prototype.setParseResponseLinks = function(iParseResponseLinks) {
		this.parseResponseLink = iParseResponseLinks;
	}

	ODatabase.prototype.getUserName = function() {
		if (!this.databaseInfo)
			return null;

		return this.databaseInfo.currentUser;
	}

	ODatabase.prototype.getUser = function() {
		var queryString = "select from OUser where name = '"
				+ this.getUserName() + "'";
		query = this.query(queryString, null, '*:-1');
		if (query == null)
			return null;

		return query.result[0];
	}

	ODatabase.prototype.getRemoveObjectCircleReferences = function() {
		return this.removeObjectCircleReferences;
	}
	ODatabase.prototype.setRemoveObjectCircleReferences = function(
			iRemoveObjectCircleReferences) {
		this.removeObjectCircleReferences = iRemoveObjectCircleReferences;
	}

	ODatabase.prototype.open = function(userName, userPass, authProxy, type) {
		if (userName == null) {
			userName = '';
		}
		if (userPass == null) {
			userPass = '';
		}
		if (authProxy != null && authProxy != '') {
			this.urlPrefix = this.databaseUrl + authProxy + "/";
		} else
			this.urlPrefix = this.databaseUrl;

		if (type == null || type == '') {
			type = 'GET';
		}
		$.ajax({
			type : type,
			url : this.urlPrefix + 'connect/' + this.encodedDatabaseName
					+ this.urlSuffix,
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			username : userName,
			password : userPass,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setDatabaseInfo(this.transformResponse(msg));
			},
			error : function(msg, textStatus, errorThrown) {
				this.setErrorMessage('Connect error: ' + msg.responseText);
				this.setDatabaseInfo(null);
			}
		});
		return this.getDatabaseInfo();
	}

	ODatabase.prototype.create = function(userName, userPass, type,
			databaseType) {
		if (userName == null)
			userName = '';

		if (userPass == null)
			userPass = '';

		if (databaseType == null)
			databaseType = 'document';

		this.urlPrefix = this.databaseUrl;

		if (type == null || type == '') {
			type = 'local';
		}
		$.ajax({
			type : "POST",
			url : this.urlPrefix + 'database/' + this.encodedDatabaseName + '/'
					+ type + '/' + databaseType + this.urlSuffix,
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			username : userName,
			password : userPass,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setDatabaseInfo(this.transformResponse(msg));
			},
			error : function(msg) {
				this.setErrorMessage('Connect error: ' + msg.responseText);
				this.setDatabaseInfo(null);
			}
		});
		return this.getDatabaseInfo();
	}

	ODatabase.prototype.query = function(iQuery, iLimit, iFetchPlan,
			successCallback) {
		if (this.databaseInfo == null) {
			this.open();
		}
		if (iLimit == null || iLimit == '') {
			iLimit = '';
		} else {
			iLimit = '/' + iLimit;
		}
		if (iFetchPlan == null || iFetchPlan == '') {
			iFetchPlan = '';
		} else {
			if (iLimit == '') {
				iLimit = '/20';
			}
			iFetchPlan = '/' + iFetchPlan;
		}
		iQuery = this.URLEncode(iQuery);
		iFetchPlan = this.URLEncode(iFetchPlan);
		$.ajax({
			type : "GET",
			url : this.urlPrefix + 'query/' + this.encodedDatabaseName + '/sql/'
					+ iQuery + iLimit + iFetchPlan + this.urlSuffix,
			context : this,
			async : false,
			contentType : "application/json; charset=utf-8",
			processData : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
				if (successCallback)
					successCallback();
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Query error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.load = function(iRID, iFetchPlan) {
		if (this.databaseInfo == null) {
			this.open();
		}

		if (iFetchPlan != null && iFetchPlan != '') {
			iFetchPlan = '/' + iFetchPlan;
		} else {
			iFetchPlan = '';
		}

		if (iRID && iRID.charAt(0) == '#')
			iRID = iRID.substring(1);

		iRID = this.URLEncode(iRID);
		$.ajax({
			type : "GET",
			url : this.urlPrefix + 'document/' + this.encodedDatabaseName + '/'
					+ iRID + iFetchPlan + this.urlSuffix,
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Query error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.save = function(obj, errorCallback, successCallback) {
		if (this.databaseInfo == null) {
			this.open();
		}

		var rid = obj['@rid'];
		var methodType = rid == null || rid == '-1:-1' ? 'POST' : 'PUT';
		if (this.removeObjectCircleReferences && typeof obj == 'object') {
			this.removeCircleReferences(obj, {});
		}
		var url = this.urlPrefix + 'document/' + this.encodedDatabaseName;
		if (rid)
			url += '/' + this.URLEncode(rid);

		$.ajax({
			type : methodType,
			url : url + this.urlSuffix,
			data : $.toJSON(obj),
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setCommandResponse(msg);
				this.setCommandResult(msg);
				if (successCallback)
					successCallback(msg.responseText);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Save error: ' + msg.responseText);
				if (errorCallback)
					errorCallback(msg.responseText);
			}
		});

		if (methodType == 'PUT') {
			return rid;
		} else {
			return this.getCommandResult();
		}
	}

	ODatabase.prototype.remove = function(obj, onsuccess, onerror) {
		if (this.databaseInfo == null)
			this.open();

		var rid;
		if (typeof obj == "string")
			rid = obj;
		else
			rid = obj['@rid'];

		rid = this.URLEncode(rid);
		$.ajax({
			type : "DELETE",
			url : this.urlPrefix + 'document/' + this.encodedDatabaseName + '/'
					+ rid + this.urlSuffix,
			contentType : "application/json; charset=utf-8",
			processData : false,
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
				if (onsuccess) {
					onsuccess();
				}
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Remove error: ' + msg.responseText);
				if (onerror) {
					onerror();
				}
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.indexPut = function(iIndexName, iKey, iValue) {
		if (this.databaseInfo == null)
			this.open();

		var req = this.urlPrefix + 'index/' + this.encodedDatabaseName + '/'
				+ iIndexName + "/" + iKey;

		var content;
		if (typeof iValue == "object")
			content = $.toJSON(iValue);
		else {
			req += "/" + iValue;
			content = null;
		}

		$.ajax({
			type : "PUT",
			url : req + this.urlSuffix,
			context : this,
			async : false,
			contentType : "application/json; charset=utf-8",
			processData : false,
			data : content,
			success : function(msg) {
				this.setErrorMessage(null);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Index put error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.indexGet = function(iIndexName, iKey) {
		if (this.databaseInfo == null)
			this.open();

		$.ajax({
			type : "GET",
			url : this.urlPrefix + 'index/' + this.encodedDatabaseName + '/'
					+ iIndexName + "/" + iKey + this.urlSuffix,
			context : this,
			async : false,
			contentType : "application/json; charset=utf-8",
			processData : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Index get error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.indexRemove = function(iIndexName, iKey) {
		if (this.databaseInfo == null)
			this.open();

		$
				.ajax({
					type : "DELETE",
					url : this.urlPrefix + 'index/' + this.encodedDatabaseName + '/'
							+ iIndexName + "/" + iKey + this.urlSuffix,
					context : this,
					async : false,
					success : function(msg) {
						this.setErrorMessage(null);
						this.handleResponse(msg);
					},
					error : function(msg) {
						this.handleResponse(null);
						this.setErrorMessage('Index remove error: '
								+ msg.responseText);
					}
				});
		return this.getCommandResult();
	}

	ODatabase.prototype.classInfo = function(iClassName) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "GET",
			url : this.urlPrefix + 'class/' + this.encodedDatabaseName + '/'
					+ iClassName + this.urlSuffix,
			context : this,
			async : false,
			contentType : "application/json; charset=utf-8",
			processData : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.createClass = function(iClassName) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "POST",
			url : this.urlPrefix + 'class/' + this.encodedDatabaseName + '/'
					+ iClassName + this.urlSuffix,
			context : this,
			async : false,
			contentType : "application/json; charset=utf-8",
			processData : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.createProperty = function(iClassName, iPropertyName,
			iPropertyType, iLinkedType) {
		if (this.databaseInfo == null) {
			this.open();
		}
		if (iPropertyType == null || iPropertyType == '') {
			iPropertyType = '';
		} else {
			iPropertyType = '/' + iPropertyType;
		}
		if (iLinkedType == null || iLinkedType == '') {
			iLinkedType = '';
		} else {
			iLinkedType = '/' + iLinkedType;
		}
		$.ajax({
			type : "POST",
			url : this.urlPrefix + 'property/' + this.encodedDatabaseName + '/'
					+ iClassName + '/' + iPropertyName + iPropertyType
					+ iLinkedType + this.urlSuffix,
			contentType : "application/json; charset=utf-8",
			processData : false,
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.createProperties = function(iClassName, iPropertiesJson) {
		if (this.databaseInfo == null) {
			this.open();
		}
		var jsonData;
		if (typeof iPropertiesJson == 'object') {
			jsonData = $.toJSON(iPropertiesJson)
		} else {
			jsonData = iPropertiesJson;
		}
		$.ajax({
			type : "POST",
			url : this.urlPrefix + 'property/' + this.encodedDatabaseName + '/'
					+ iClassName + this.urlSuffix,
			context : this,
			data : jsonData,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.browseCluster = function(iClassName) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "GET",
			url : this.urlPrefix + 'cluster/' + this.encodedDatabaseName + '/'
					+ iClassName + this.urlSuffix,
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.executeCommand = function(iCommand, iLanguage, iLimit) {
		if (this.databaseInfo == null)
			this.open();

		if (!iLanguage)
			iLanguage = "sql";

		if (!iLimit)
			iLimit = -1;

		var dataType = this.evalResponse ? null : 'text';

		iCommand = this.URLEncode(iCommand);
		$.ajax({
			type : "POST",
			url : this.urlPrefix + 'command/' + this.encodedDatabaseName + '/'
					+ iLanguage + '/' + iCommand + "/" + iLimit
					+ this.urlSuffix,
			context : this,
			async : false,
			'dataType' : dataType,
			contentType : "application/json; charset=utf-8",
			processData : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResponse();
	}
	

	ODatabase.prototype.executeFunction = function(iName, iParameters) {
		if (this.databaseInfo == null)
			this.open();

		var dataType = this.evalResponse ? null : 'text';
		
		var params = "";
		if( iParameters )
			for( p in iParameters )
				params += '/' + iParameters[p];

		iName = this.URLEncode(iName);
		$.ajax({
			type : "POST",
			url : this.urlPrefix + 'function/' + this.encodedDatabaseName + '/'
					+ iName + params
					+ this.urlSuffix,
			context : this,
			async : false,
			'dataType' : dataType,
			contentType : "application/json; charset=utf-8",
			processData : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Function error: ' + msg.responseText);
			}
		});
		return this.getCommandResponse();
	}


	ODatabase.prototype.serverInfo = function() {
		$.ajax({
			type : "GET",
			url : this.urlPrefix + 'server' + this.urlSuffix,
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.profiler = function(type, from, to) {
		if (!type)
			type = 'realtime';

		$.ajax({
			type : "GET",
			url : this.urlPrefix + 'profiler/' + this.urlSuffix,
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.listDatabases = function() {
		$.ajax({
			type : "GET",
			url : this.databaseUrl + '/' + 'listDatabases' + this.urlSuffix,
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.schema = function() {
		if (this.databaseInfo == null) {
			this.setErrorMessage('Database is closed');
			return null;
		}
		return this.transformResponse(this.getDatabaseInfo())['classes'];
	}

	ODatabase.prototype.getClass = function(className) {
		var classes = this.databaseInfo['classes'];
		for ( var cls in classes) {
			if (!classes.hasOwnProperty(cls)) {
				continue;
			}
			if (classes[cls].name == className) {
				return classes[cls];
			}
		}
		return null;
	}

	ODatabase.prototype.securityRoles = function() {
		if (this.databaseInfo == null) {
			this.setErrorMessage('Database is closed');
			return null;
		}
		return this.transformResponse(this.getDatabaseInfo())['roles'];
	}

	ODatabase.prototype.securityUsers = function() {
		if (this.databaseInfo == null) {
			this.setErrorMessage('Database is closed');
			return null;
		}
		return this.transformResponse(this.getDatabaseInfo())['users'];
	}

	ODatabase.prototype.close = function() {
		if (this.databaseInfo != null) {
			$.ajax({
				type : 'GET',
				url : this.urlPrefix + 'disconnect' + this.urlSuffix,
				dataType : "json",
				contentType : "application/json; charset=utf-8",
				processData : false,
				async : false,
				context : this,
				success : function(msg) {
					this.handleResponse(msg);
					this.setErrorMessage(null);
				},
				error : function(msg) {
					this.handleResponse(null);
					this.setErrorMessage('Command response: '
							+ msg.responseText);
				}
			});
		}
		this.databaseInfo = null;
		return this.getCommandResult();
	}

	ODatabase.prototype.importRecords = function(content, configuration,
			errorCallback, successCallback) {
		if (this.databaseInfo == null)
			this.open();

		var cfg = {
			"format" : "CSV",
			"separator" : ",",
			"stringDelimiter" : '"',
			"decimalSeparator" : ".",
			"thousandsSeparator" : ","
		}

		if (configuration)
			// OVERWRITE DEFAULT CONFIGURATION
			for ( var c in configuration) {
				if (!configuration.hasOwnProperty(c)) {
					continue;
				}
				cfg[c] = configuration[c];
			}

		$.ajax({
			type : "POST",
			url : this.urlPrefix + 'importRecords/' + $('#header-database').val()
					+ '/' + cfg["format"] + '/' + cfg["class"] + '/'
					+ cfg["separator"] + '/' + cfg["stringDelimiter"]
					+ cfg["decimalSeparator"] + '/' + cfg["thousandsSeparator"]
					+ '/' + this.urlSuffix,
			data : content,
			context : this,
			contentType : "application/json; charset=utf-8",
			processData : false,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setCommandResponse(msg);
				this.setCommandResult(msg);
				if (successCallback)
					successCallback(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Import error: ' + msg.responseText);
				if (errorCallback)
					errorCallback(msg);
			}
		});
	}

	ODatabase.prototype.handleResponse = function(iResponse) {
		if (typeof iResponse != 'object') {
			iResponse = this.UTF8Encode(iResponse);
		}
		this.setCommandResponse(iResponse);
		if (iResponse != null)
			this.setCommandResult(this.transformResponse(iResponse));
		else
			this.setCommandResult(null);
	}

	ODatabase.prototype.transformResponse = function(msg) {
		if (this.getEvalResponse()) {
			var returnValue;
			if (msg.length > 0 && typeof msg != 'object') {
				returnValue = jQuery.parseJSON(msg)
			} else {
				returnValue = msg;
			}
			if (this.getParseResponseLinks()) {
				return this.parseConnections(returnValue);
			} else {
				return returnValue;
			}
		} else {
			return msg;
		}
	}

	ODatabase.prototype.parseConnections = function(obj) {
		if (typeof obj == 'object') {
			var linkMap = {
				"foo" : 0
			};
			linkMap = this.createObjectsLinksMap(obj, linkMap);
			if (linkMap["foo"] == 1) {
				linkMap = this.putObjectInLinksMap(obj, linkMap);
				if (linkMap["foo"] == 2) {
					obj = this.getObjectFromLinksMap(obj, linkMap);
				}
			}
		}
		return obj;
	}

	ODatabase.prototype.createObjectsLinksMap = function(obj, linkMap) {
		for ( var field in obj) {
			if (!obj.hasOwnProperty(field)) {
				continue;
			}
			var value = obj[field];
			if (typeof value == 'object') {
				this.createObjectsLinksMap(value, linkMap);
			} else {
				if (typeof value == 'string') {
					if (value.length > 0 && value.charAt(0) == '#') {
						if (!linkMap.hasOwnProperty(value)) {
							linkMap["foo"] = 1;
							linkMap[value] = null;
						}
					}
				}
			}
		}
		return linkMap;
	}

	ODatabase.prototype.putObjectInLinksMap = function(obj, linkMap) {
		for ( var field in obj) {
			if (!obj.hasOwnProperty(field)) {
				continue;
			}
			var value = obj[field];
			if (typeof value == 'object') {
				this.putObjectInLinksMap(value, linkMap);
			} else {
				if (field == '@rid' && value.length > 0
						&& linkMap.hasOwnProperty(value)
						&& linkMap[value] === null) {
					linkMap["foo"] = 2;
					linkMap[value] = obj;
				}
			}
		}
		return linkMap;
	}

	ODatabase.prototype.getObjectFromLinksMap = function(obj, linkMap) {
		for ( var field in obj) {
			if (!obj.hasOwnProperty(field)) {
				continue;
			}
			var value = obj[field];
			if (typeof value == 'object') {
				this.getObjectFromLinksMap(value, linkMap);
			} else {
				if (field != '@rid' && value.length > 0
						&& value.charAt(0) == '#' && linkMap[value] != null) {
					obj[field] = linkMap[value];
				}
			}
		}
		return obj;
	}

	ODatabase.prototype.removeCircleReferences = function(obj, linkMap) {
		linkMap = this.removeCircleReferencesPopulateMap(obj, linkMap);
		if (obj != null && typeof obj == 'object' && !$.isArray(obj)) {
			if (obj['@rid'] != null && obj['@rid']) {
				var rid = this.getRidWithPound(obj['@rid']);
				linkMap[rid] = rid;
			}
		}
		this.removeCircleReferencesChangeObject(obj, linkMap);
	}

	ODatabase.prototype.removeCircleReferencesPopulateMap = function(obj,
			linkMap) {
		for ( var field in obj) {
			if (!obj.hasOwnProperty(field)) {
				continue;
			}
			var value = obj[field];
			if (value != null && typeof value == 'object' && !$.isArray(value)) {
				if (value['@rid'] != null && value['@rid']) {
					var rid = this.getRidWithPound(value['@rid']);
					if (linkMap[rid] == null || !linkMap[rid]) {
						linkMap[rid] = value;
					}
					linkMap = this.removeCircleReferencesPopulateMap(value,
							linkMap);
				}
			} else if (value != null && typeof value == 'object'
					&& $.isArray(value)) {
				for ( var i in value) {
					if (!value.hasOwnProperty(i)) {
						continue;
					}
					var arrayValue = value[i];
					if (arrayValue != null && typeof arrayValue == 'object') {
						if (arrayValue['@rid'] != null && arrayValue['@rid']) {
							var rid = this.getRidWithPound(arrayValue['@rid']);
							if (linkMap[rid] == null || !linkMap[rid]) {
								linkMap[rid] = arrayValue;
							}
						}
						linkMap = this.removeCircleReferencesPopulateMap(
								arrayValue, linkMap);
					}
				}
			}
		}
		return linkMap;
	}

	ODatabase.prototype.removeCircleReferencesChangeObject = function(obj,
			linkMap) {
		for ( var field in obj) {
			if (!obj.hasOwnProperty(field)) {
				continue;
			}
			var value = obj[field];
			if (value != null && typeof value == 'object' && !$.isArray(value)) {
				var inspectObject = true;
				if (value['@rid'] != null && value['@rid']) {
					var rid = this.getRidWithPound(value['@rid']);
					if (linkMap[rid] != null && linkMap[rid]) {
						var mapValue = linkMap[rid];
						if (typeof mapValue == 'object') {
							linkMap[rid] = rid;
						} else {
							obj[field] = mapValue;
							inspectObject = false;
						}
					}
				}
				if (inspectObject) {
					this.removeCircleReferencesChangeObject(value, linkMap);
				}
			} else if (value != null && typeof value == 'object'
					&& $.isArray(value)) {
				for ( var i in value) {
					if (!value.hasOwnProperty(i)) {
						continue;
					}
					var arrayValue = value[i];
					if (typeof arrayValue == 'object') {
						var inspectObject = true;
						if (arrayValue['@rid'] != null && arrayValue['@rid']) {
							var rid = this.getRidWithPound(arrayValue['@rid']);
							if (linkMap[rid] != null && linkMap[rid]) {
								var mapValue = linkMap[rid];
								if (typeof mapValue == 'object') {
									linkMap[rid] = rid;
								} else {
									value[i] = mapValue;
									inspectObject = false;
								}
							}
						}
						if (inspectObject) {
							this.removeCircleReferencesChangeObject(arrayValue,
									linkMap);
						}
					}
				}
			}
		}
	}

	ODatabase.prototype.getRidWithPound = function(rid) {
		if (rid.indexOf('#', 0) > -1) {
			return rid;
		} else {
			return '#' + rid;
		}
	}

	ODatabase.prototype.URLEncode = function(c) {
		var o = '';
		var x = 0;
		c = c.toString();
		var r = /(^[a-zA-Z0-9_.]*)/;
		while (x < c.length) {
			var m = r.exec(c.substr(x));
			if (m != null && m.length > 1 && m[1] != '') {
				o += m[1];
				x += m[1].length;
			} else {
				if (c[x] == ' ')
					o += '+';
				else {
					var d = c.charCodeAt(x);
					var h = d.toString(16);
					o += '%' + (h.length < 2 ? '0' : '') + h.toUpperCase();
				}
				x++;
			}
		}
		return o;
	}

	ODatabase.prototype.URLDecode = function(s) {
		var o = s;
		var binVal, t;
		var r = /(%[^%]{2})/;
		while ((m = r.exec(o)) != null && m.length > 1 && m[1] != '') {
			b = parseInt(m[1].substr(1), 16);
			t = String.fromCharCode(b);
			o = o.replace(m[1], t);
		}
		return o;
	}

	ODatabase.prototype.URLDecodeU = function(string) {
		string = string.replace(/\r\n/g, "\n");
		var utftext = "";

		for ( var n = 0; n < string.length; n++) {

			var c = string.charCodeAt(n);

			if (c < 128) {
				utftext += String.fromCharCode(c);
			} else if ((c > 127) && (c < 2048)) {
				utftext += String.fromCharCode((c >> 6) | 192);
				utftext += String.fromCharCode((c & 63) | 128);
			} else {
				utftext += String.fromCharCode((c >> 12) | 224);
				utftext += String.fromCharCode(((c >> 6) & 63) | 128);
				utftext += String.fromCharCode((c & 63) | 128);
			}

		}

		return utftext;
	}

	ODatabase.prototype.UTF8Encode = function(string) {
		string = string.replace(/\r\n/g, "\n");
		var utftext = "";

		for ( var n = 0; n < string.length; n++) {

			var c = string.charCodeAt(n);

			if (c < 128) {
				utftext += String.fromCharCode(c);
			} else if ((c > 127) && (c < 2048)) {
				utftext += String.fromCharCode((c >> 6) | 192);
				utftext += String.fromCharCode((c & 63) | 128);
			} else {
				utftext += String.fromCharCode((c >> 12) | 224);
				utftext += String.fromCharCode(((c >> 6) & 63) | 128);
				utftext += String.fromCharCode((c & 63) | 128);
			}

		}

		return utftext;
	}

	ODatabase.prototype.UTF8Decode = function(utftext) {
		var string = "";
		var i = 0;
		var c = c1 = c2 = 0;

		while (i < utftext.length) {

			c = utftext.charCodeAt(i);

			if (c < 128) {
				string += String.fromCharCode(c);
				i++;
			} else if ((c > 191) && (c < 224)) {
				c2 = utftext.charCodeAt(i + 1);
				string += String.fromCharCode(((c & 31) << 6) | (c2 & 63));
				i += 2;
			} else {
				c2 = utftext.charCodeAt(i + 1);
				c3 = utftext.charCodeAt(i + 2);
				string += String.fromCharCode(((c & 15) << 12)
						| ((c2 & 63) << 6) | (c3 & 63));
				i += 3;
			}

		}

		return string;
	}
}
