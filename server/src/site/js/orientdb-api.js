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
	this.urlPrefix = "";

	if (databasePath) {
		var pos = databasePath.indexOf('orientdb_proxy', 8); // JUMP HTTP
		if (pos > -1) {
			pos = databasePath.indexOf('/', pos); // END OF PROXY
		} else {
			pos = databasePath.indexOf('/', 8);
		}

		this.databaseUrl = databasePath.substring(0, pos + 1);
		this.databaseName = databasePath.substring(pos + 1);
		if (this.databaseName.indexOf('/') > -1) {
			this.encodedDatabaseName = "";
			var parts = this.databaseName.split('/');
			for (p in parts) {
				if (this.encodedDatabaseName.length > 0)
					this.encodedDatabaseName += '$';
				this.encodedDatabaseName += parts[p];
			}
		} else
		this.encodedDatabaseName = this.databaseName;
	}

	ODatabase.prototype.getDatabaseInfo = function() {
		return this.databaseInfo;
	}
	ODatabase.prototype.setDatabaseInfo = function(iDatabaseInfo) {
		this.databaseInfo = iDatabaseInfo;
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
		return databaseUrl;
	}
	ODatabase.prototype.setDatabaseUrl = function(iDatabaseUrl) {
		this.databaseUrl = iDatabaseUrl;
	}

	ODatabase.prototype.getDatabaseName = function() {
		return this.encodedDatabaseName;
	}
	ODatabase.prototype.setDatabaseName = function(iDatabaseName) {
		this.databaseName = iDatabaseName;
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
		return this.databaseInfo.currentUser;
	}
	
	ODatabase.prototype.getUser = function() {
		var queryString = "select from OUser where name = '" + this.getUserName() + "'";
		query = this.query(queryString, null, '*:-1');
		var dbUser = query.result[0];
		queryString = "select from HResource where dbUser = " + dbUser['@rid'];;
		query = window.database.query(queryString, null, '*:-1');
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
			urlPrefix = this.databaseUrl + authProxy + "/";
		} else
			urlPrefix = this.databaseUrl;

		if (type == null || type == '') {
			type = 'GET';
		}
		$.ajax({
			type : type,
			url : urlPrefix + 'connect/' + this.encodedDatabaseName,
			context : this,
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
	


	ODatabase.prototype.create = function(userName, userPass, type) {
		if (userName == null) {
			userName = '';
		}
		if (userPass == null) {
			userPass = '';
		}
		urlPrefix = this.databaseUrl;

		if (type == null || type == '') {
			type = 'local';
		}
		$.ajax({
			type : "POST",
			url : urlPrefix + 'database/' + this.encodedDatabaseName
			+ '/' + type,
			context : this,
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

	ODatabase.prototype.query = function(iQuery, iLimit, iFetchPlan) {
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
			url : urlPrefix + 'query/' + this.encodedDatabaseName + '/sql/'
					+ iQuery + iLimit + iFetchPlan,
			context : this,
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
			url : urlPrefix + 'document/' + this.encodedDatabaseName + '/'
					+ iRID + iFetchPlan,
			context : this,
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

	ODatabase.prototype.save = function(obj, errorCallback) {
		if (this.databaseInfo == null) {
			this.open();
		}
	
		var rid = obj['@rid'];
		var methodType = rid == null || rid == '-1:-1' ? 'POST' : 'PUT';
		if (this.removeObjectCircleReferences && typeof obj == 'object') {
			this.removeCircleReferences(obj, {});
		}
		var url = urlPrefix + 'document/' + this.encodedDatabaseName;
		if (rid)
			url += '/' + this.URLEncode(rid);
		
		$.ajax({
			type : methodType,
			url : url,
			data : $.toJSON(obj),
			processData : false,
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setCommandResponse(msg);
				this.setCommandResult(msg);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Save error: ' + msg.responseText);
				errorCallback();
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
			url : urlPrefix + 'document/' + this.encodedDatabaseName + '/'
					+ rid,
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

		var req = urlPrefix + 'index/' + this.encodedDatabaseName + '/'
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
			url : req,
			context : this,
			async : false,
			data : content,
			success : function(msg) {
				this.setErrorMessage(null);
				this.handleResponse(msg);
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
			url : urlPrefix + 'index/' + this.encodedDatabaseName + '/'
					+ iIndexName + "/" + iKey,
			context : this,
			async : false,
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
					url : urlPrefix + 'index/' + this.encodedDatabaseName + '/'
							+ iIndexName + "/" + iKey,
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
			url : urlPrefix + 'class/' + this.encodedDatabaseName + '/'
					+ iClassName,
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

	ODatabase.prototype.createClass = function(iClassName) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "POST",
			url : urlPrefix + 'class/' + this.encodedDatabaseName + '/'
					+ iClassName,
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

	ODatabase.prototype.browseCluster = function(iClassName) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "GET",
			url : urlPrefix + 'cluster/' + this.encodedDatabaseName + '/'
					+ iClassName,
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
			url : urlPrefix + 'command/' + this.encodedDatabaseName + '/'
					+ iLanguage + '/' + iCommand + "/" + iLimit,
			context : this,
			async : false,
			'dataType' : dataType,
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

	ODatabase.prototype.serverInfo = function() {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "GET",
			url : urlPrefix + 'server',
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

	ODatabase.prototype.listDatabases = function() {
		$.ajax({
			type : "GET",
			url : this.databaseUrl + '/' + 'listDatabases',
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

	ODatabase.prototype.schema = function() {
		if (this.databaseInfo == null) {
			this.setErrorMessage('Database is closed');
			return null;
		}
		return this.transformResponse(this.getDatabaseInfo())['classes'];
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
				url : urlPrefix + 'disconnect',
				dataType : "json",
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

	ODatabase.prototype.handleResponse = function(iResponse) {
		if (typeof iResponse != 'object')
			iResponse = this.URLDecodeU(iResponse);
		this.setCommandResponse(iResponse);
		if (iResponse != null)
			this.setCommandResult(this.transformResponse(iResponse));
		else
			this.setCommandResult(null);
	}

	ODatabase.prototype.transformResponse = function(msg) {
		if (this.getEvalResponse() && msg.length > 0 && typeof msg != 'object') {
			if (this.getParseResponseLinks()) {
				return this.parseConnections(jQuery.parseJSON(msg));
			} else {
				return jQuery.parseJSON(msg);
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
		for (field in obj) {
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
		for (field in obj) {
			var value = obj[field];
			if (typeof value == 'object') {
				this.putObjectInLinksMap(value, linkMap);
			} else {
				if (field == '@rid' && value.length > 0
						&& linkMap.hasOwnProperty("#" + value)
						&& linkMap["#" + value] === null) {
					linkMap["foo"] = 2;
					linkMap["#" + value] = obj;
				}
			}
		}
		return linkMap;
	}

	ODatabase.prototype.getObjectFromLinksMap = function(obj, linkMap) {
		for (field in obj) {
			var value = obj[field];
			if (typeof value == 'object') {
				this.getObjectFromLinksMap(value, linkMap);
			} else {
				if (value.length > 0 && value.charAt(0) == '#'
						&& linkMap[value] != null) {
					obj[field] = linkMap[value];
				}
			}
		}
		return obj;
	}

	ODatabase.prototype.removeCircleReferences = function(obj, linkMap) {
		linkMap = this.removeCircleReferencesPopulateMap(obj, linkMap);
		this.removeCircleReferencesChangeObject(obj, linkMap);
	}

	ODatabase.prototype.removeCircleReferencesPopulateMap = function(obj,
			linkMap) {
		for (field in obj) {
			var value = obj[field];
			if (value!=null && typeof value == 'object' && !$.isArray(value)) {
				if (value['@rid'] != null && value['@rid']) {
					var rid = this.getRidWithPound(value['@rid']);
					if (linkMap[rid] == null || !linkMap[rid]) {
						linkMap[rid] = value;
					}
					linkMap = this.removeCircleReferencesPopulateMap(value,
							linkMap);
				}
			} else if (value!=null && typeof value == 'object' && $.isArray(value)) {
				for (i in value) {
					var arrayValue = value[i];
					if (arrayValue!=null && typeof arrayValue == 'object') {
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
		for (field in obj) {
			var value = obj[field];
			if (value!=null && typeof value == 'object' && !$.isArray(value)) {
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
			} else if (value!=null && typeof value == 'object' && $.isArray(value)) {
				for (i in value) {
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

	/*
	 * !
	 * 
	 * jQuery URL Encoder Decoder Plugin
	 * 
	 * http://0061276.netsolhost.com/tony/testurl.html
	 * 
	 */
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

}
