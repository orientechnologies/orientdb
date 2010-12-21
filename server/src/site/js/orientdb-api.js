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
function ODatabase() {
	this.databaseInfo = null;
	this.queryResult = null;
	this.commandResult = null;
	this.errorMessage = null;
	this.databaseUrl = "";
	this.databaseName = "";
	this.userName = "";
	this.userPass = "";

	this.getDatabaseInfo = function() {
		return this.databaseInfo;
	}
	this.setDatabaseInfo = function(iDatabaseInfo) {
		this.databaseInfo = iDatabaseInfo;
	}

	this.getQueryResult = function() {
		return this.queryResult;
	}
	this.setQueryResult = function(iQueryResult) {
		this.queryResult = iQueryResult;
	}

	this.getCommandResult = function() {
		return this.commandResult;
	}
	this.setCommandResult = function(iCommandResult) {
		this.commandResult = iCommandResult;
	}

	this.getErrorMessage = function() {
		return this.errorMessage;
	}
	this.setErrorMessage = function(iErrorMessage) {
		this.errorMessage = iErrorMessage;
	}

	this.getDatabaseUrl = function() {
		return databaseUrl;
	}
	this.setDatabaseUrl = function(iDatabaseUrl) {
		this.databaseUrl = iDatabaseUrl;
	}

	this.getDatabaseName = function() {
		return this.databaseName;
	}
	this.setDatabaseName = function(iDatabaseName) {
		this.databaseName = iDatabaseName;
	}

	this.getUserName = function() {
		return this.userName;
	}
	this.setUserName = function(iUserName) {
		this.userName = iUserName;
	}

	this.getUserPass = function() {
		return this.userPass;
	}
	this.setUserPass = function(iUserPass) {
		this.userPass = iUserPass;
	}

	this.executeJSONRequest = function(iRequest, iCallback, iData, iMethod,
			iContext) {
		if (!iMethod)
			iMethod = 'GET';
		$.ajax({
			type : iMethod,
			url : iRequest,
			context : iContext,
			async : false,
			success : function(msg) {
				iCallback.apply($(this), [ jQuery.parseJSON(msg) ]);
			},
			data : iData,
			error : function(msg) {
				return msg;
			}
		});
	}

	this.open = function() {
		$.ajax({
			type : "GET",
			url : this.databaseUrl + '/connect/' + this.databaseName,
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setDatabaseInfo(jQuery.parseJSON(msg));
			},
			error : function(msg) {
				this.setErrorMessage('Connect error: ' + msg);
			}
		});
	}

	this.query = function(iQuery) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "GET",
			url : this.databaseUrl + '/query/' + this.databaseName + '/sql/'
					+ iQuery,
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setQueryResult(jQuery.parseJSON(msg));
			},
			error : function(msg) {
				this.setErrorMessage('Query error: ' + msg.responseText);
			}
		});
	}

	this.classInfo = function(iClassName) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "POST",
			url : this.databaseUrl + '/class/' + this.databaseName + '/'
					+ iClassName,
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setCommandResult(jQuery.parseJSON(msg));
			},
			error : function(msg) {
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
	}

	this.browseCluster = function(iClassName) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "POST",
			url : this.databaseUrl + '/cluster/' + this.databaseName + '/'
					+ iClassName,
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setCommandResult(jQuery.parseJSON(msg));
			},
			error : function(msg) {
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
	}

	this.executeCommand = function(iCommand) {
		if (this.databaseInfo == null) {
			this.open();
		}
		$.ajax({
			type : "POST",
			url : this.databaseUrl + '/command/' + this.databaseName + '/sql/'
					+ iCommand + "/",
			context : this,
			async : false,
			success : function(msg) {
				this.setErrorMessage(null);
				this.setCommandResult(jQuery.parseJSON(msg));
			},
			error : function(msg) {
				this.setErrorMessage('Command error: ' + msg.responseText);
			}
		});
	}

	this.close = function() {
		if (this.databaseInfo != null) {
			$.ajax({
				type : 'GET',
				url : this.databaseUrl + '/disconnect',
				dataType : "json",
				async : false,
				success : function(msg) {
					return jQuery.parseJSON(msg);
				},
				error : function(msg) {
					this.setErrorMessage('Command response: '
							+ msg.responseText);
				}
			});
		}
		this.databaseInfo = null;
	}
}
