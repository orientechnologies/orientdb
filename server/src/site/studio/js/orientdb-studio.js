/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

// GLOBAL VARIABLES
var queryEditor;
var commandEditor;
var graphEditor;

function startTimer() {
	startTime = new Date().getTime();
}

function stopTimer() {
	return ((new Date().getTime() - startTime) / 1000);
}

function getStudioURL(context) {
	return $('#header-server').val() + '/studio/'
			+ $('#header-database').val().replace(/\//g, '$') + '/' + context;
}

function clear(component) {
	$('#' + component).val("");
}

function formatServerURL() {
	var s = $('#server').val();

	var index = s.indexOf('orientdb_proxy', 8); // JUMP HTTP://
	if (index > -1) {
		index = s.indexOf('/', index); // END OF PROXY
		$('#server').val(s.substring(0, index));
	} else {
		index = s.indexOf('/', 8); // JUMP HTTP://
		if (index > -1)
			$('#server').val(s.substring(0, index));
	}

	$('#rawServer').html($('#server').val() + "/");
}

function getRequestParams(s) {
	if (s == null) {
		s = document.location.search;
		if (s == null || s == '') {
			s = document.location.hash;
		}
	}
	if (s.match(/^\?/) || s.match(/^#/)) {
		s = s.substring(1);
	}
	var strParams = s.split('&');
	var params = {};
	var i = 0;
	for (i in strParams) {
		var name = strParams[i];
		var value = true;
		var pos = name.indexOf('=');
		if (pos > 0) {
			value = decodeURIComponent(name.substring(pos + 1));
			name = name.substring(0, pos);
		}
		params[name] = value;
	}
	return params;
};

function getRequestParam(name, string) {
	var params = getRequestParams(string);
	return params[name] || "";
}

function generateClassSelect(id, selectedClass) {
	var classes = "<select id='" + id + "'>";
	for (cls in databaseInfo['classes']) {
		classes += "<option";
		if (selectedClass && databaseInfo['classes'][cls].name == selectedClass) {
			currentClass = databaseInfo['classes'][cls];
			classes += " selected = 'yes'";
		}
		classes += ">" + databaseInfo['classes'][cls].name + "</option>";
	}
	classes += "</select>";
	return classes;
}

$(document).ready(function() {
	$(document).ajaxError(function(event, request, settings, err) {
		$("#output").val("Error: " + request.responseText);
	});

	$('#header').hide();
	$("#navigation").hide();
	$("#buttonDisconnect").hide();

	$("#database").blur(function() {
		$('#rawDatabase').val($("#database").val());
	});

	$("#output").val($.trim($("#output").val()));

	$("#tabs-main").hide();
	$("#buttonDisconnect").hide();

	controller.loadFragment('panelHome.htm');
});
