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
		return this.databaseName;
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

		if (iRID.charAt(0) == '#')
			iRID = iRID.substring(1);

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

	ODatabase.prototype.save = function(obj) {
		if (this.databaseInfo == null) {
			this.open();
		}

		var rid = obj['@rid'];
		var methodType = rid == null || rid == '-1:-1' ? 'POST' : 'PUT';

		$.ajax({
			type : methodType,
			url : urlPrefix + 'document/' + this.encodedDatabaseName + '/'
					+ rid,
			data : $.toJSON(obj),
			processData : false,
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setCommandResponse(msg);
				this.setCommandResult(null);
			},
			error : function(msg) {
				this.handleResponse(null);
				this.setErrorMessage('Save error: ' + msg.responseText);
			}
		});
		return this.getCommandResult();
	}

	ODatabase.prototype.remove = function(obj, onsuccess, onerror) {
		if (this.databaseInfo == null)
			this.open();

		var rid;
		if (typeof obj == "string")
			rid = obj;
		else
			rid = obj['@rid'];

		$.ajax({
			type : 'DELETE',
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
				this.setErrorMessage('Save error: ' + msg.responseText);
				if (onerror) {
					onerror();
				}
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

	ODatabase.prototype.executeCommand = function(iCommand) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "POST",
			url : urlPrefix + 'command/' + this.encodedDatabaseName + '/sql/'
					+ iCommand + "/",
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
			var linkMap = {"foo" : 0};
			linkMap = this.createObjectsLinksMap(obj, linkMap);
			if (linkMap["foo"] == 1 ) {
				linkMap = this.putObjectInLinksMap(obj, linkMap);
				obj = this.getObjectFromLinksMap(obj, linkMap);
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
				if (value.length > 0 && value.charAt(0) == '#') {
					obj[field] = linkMap[value];
				}
			}
		}
		return obj;
	}

}
