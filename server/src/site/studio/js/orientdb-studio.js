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
var selectedObject;

function startTimer() {
	startTime = new Date().getTime();
}

function stopTimer() {
	return ((new Date().getTime() - startTime) / 1000);
}

function getStudioURL(context) {
	return $('#header-server').val() + '/studio/' + $('#header-database').val() + '/'
			+ context;
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

jQuery(document).ready(function() {
	jQuery(document).ajaxError(function(event, request, settings, err) {
		jQuery("#output").val("Error: " + request.responseText);
	});

	$('#header').hide();
	$("#navigation").hide();
	$("#buttonDisconnect").hide();

	$("#database").blur(function() {
		$('#rawDatabase').val($("#database").val());
	});

	jQuery("#output").val(jQuery.trim(jQuery("#output").val()));

	$("#tabs-main").hide();
	$("#buttonDisconnect").hide();

	controller.loadFragment('panelHome.htm');
});
