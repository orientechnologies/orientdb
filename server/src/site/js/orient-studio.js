// GLOBAL VARIABLES
var version = "0.9.14";
var startTime = 0; // CONTAINS THE LAST EXECUTION TIME
var databaseInfo; // CONTAINS THE DB INFO
var classEnumeration; // CONTAINS THE DB CLASSES
var selectedClassName; // CONTAINS LATEST SELECTED CLASS NAME

function connect() {
	executeJSONRequest($('#server').val() + '/connect/' + $('#database').val(),
			function(database) {
				databaseInfo = database;
				showDatabaseInfo(database);

				$("#tabs-main").show(200);
				$("#buttonConnect").hide();
				$("#buttonDisconnect").show();

			});
}

function disconnect() {
	executeSimpleRequest($('#server').val() + '/disconnect',
			function(database) {
				databaseInfo = null;
				$("#tabs-main").hide(200);
				$("#buttonConnect").show();
				$("#buttonDisconnect").hide();
			});
}

function showDatabaseInfo(database) {
	databaseInfo = database;

	fillDynaTable($('#databaseDataSegments'), "Data Segments", [ 'id', 'name',
			'size', 'filled', 'maxSize', 'files' ], [ {
		name : 'id',
		index : 'id',
		width : '30px',
		editable : false
	}, {
		name : 'name',
		index : 'name',
		width : '100px',
		editable : false
	}, {
		name : 'size',
		index : 'size',
		width : '80px',
		editable : false
	}, {
		name : 'filled',
		index : 'filled',
		width : '80px',
		editable : false
	}, {
		name : 'maxSize',
		index : 'maxSize',
		width : '80px',
		editable : false
	}, {
		name : 'files',
		index : 'files',
		width : '600px',
		editable : false
	} ], database['dataSegments'], {
		height : '40px'
	});

	fillDynaTable($('#databaseClusters'), "Clusters", [ 'id', 'name', 'type',
			'records', 'size', 'filled', 'maxSize', 'files' ], [ {
		name : 'id',
		index : 'id',
		width : '30px',
		editable : false
	}, {
		name : 'name',
		index : 'name',
		width : '100px',
		editable : false
	}, {
		name : 'type',
		index : 'type',
		width : '80px',
		editable : false
	}, {
		name : 'records',
		index : 'records',
		width : '80px',
		editable : false
	}, {
		name : 'size',
		index : 'size',
		width : '80px',
		editable : false
	}, {
		name : 'filled',
		index : 'filled',
		width : '80px',
		editable : false
	}, {
		name : 'maxSize',
		index : 'maxSize',
		width : '80px',
		editable : false
	}, {
		name : 'files',
		index : 'files',
		width : '450px',
		editable : false
	} ], database['clusters']);

	fillDynaTable($('#databaseTxSegment'), "Tx Segment", [ 'totalLogs', 'size',
			'filled', 'maxSize', 'file' ], [ {
		name : 'totalLogs',
		index : 'Total Logs',
		width : '80px',
		editable : false
	}, {
		name : 'size',
		index : 'Size',
		width : '80px',
		editable : false
	}, {
		name : 'filled',
		index : 'Filled',
		width : '80px',
		editable : false
	}, {
		name : 'maxSize',
		index : 'Max Size',
		width : '80px',
		editable : false
	}, {
		name : 'file',
		index : 'File',
		width : '650px',
		editable : false
	} ], database['txSegment'], {
		height : '25px'
	});

	fillDynaTable($('#databaseUsers'), "Users", [ 'name', 'roles' ], null,
			database['users']);
	fillDynaTable($('#databaseRolesRules'), "Rules", [ 'Name', 'Create',
			'Read', 'Update', 'Delete' ], [ {
		name : 'name',
		index : 'name',
		formatter : 'text',
		edittype : 'text',
		editable : false
	}, {
		name : 'create',
		index : 'create',
		formatter : 'checkbox',
		edittype : 'checkbox',
		editable : true
	}, {
		name : 'read',
		index : 'read',
		formatter : 'checkbox',
		edittype : 'checkbox',
		editable : true
	}, {
		name : 'update',
		index : 'update',
		formatter : 'checkbox',
		edittype : 'checkbox',
		editable : true
	}, {
		name : 'delete',
		index : 'delete',
		formatter : 'checkbox',
		edittype : 'checkbox',
		editable : true
	} ], null);
	fillDynaTable($('#databaseRoles'), "Roles", [ 'name', 'mode' ], null,
			database['roles'], {
				sortname : 'id',
				onSelectRow : function(roleRowNum) {
					var role = database['roles'][roleRowNum - 1];
					fillDynaTableRows($('#databaseRolesRules'), role.rules);
				}
			});

	classEnumeration = ":;";
	for (cls in database['classes']) {
		if (classEnumeration.length > 2)
			classEnumeration += ";";
		classEnumeration += database['classes'][cls].name + ":"
				+ database['classes'][cls].name;
	}

	fillDynaTable(
			$('#classProperties'),
			"Properties",
			[ 'id', 'name', 'type', 'linkedType', 'linkedClass', 'mandatory',
					'notNull', 'min', 'max' ],
			[
					{
						name : 'id',
						index : 'id',
						width : 30
					},
					{
						name : 'name',
						index : 'name',
						width : 180,
						editable : true,
						editrules : {
							required : true
						},
						formoptions : {
							elmprefix : '(*)'
						}
					},
					{
						name : 'type',
						index : 'type',
						width : 100,
						edittype : "select",
						editoptions : {
							value : ":;BINARY:BINARY;BOOLEAN:BOOLEAN;EMBEDDED:EMBEDDED;EMBEDDEDLIST:EMBEDDEDLIST;EMBEDDEDMAP:EMBEDDEDMAP;EMBEDDEDSET:EMBEDDEDSET;FLOAT:FLOAT;DATE:DATE;DOUBLE:DOUBLE;INTEGER:INTEGER;LINK:LINK;LINKLIST:LINKLIST;LINKMAP:LINKMAP;LINKSET:LINKSET;LONG:LONG;SHORT:SHORT;STRING:STRING"
						},
						editable : true,
						editrules : {
							required : true
						},
						formoptions : {
							elmprefix : '(*)'
						}
					},
					{
						name : 'linkedType',
						index : 'linkedType',
						width : 150,
						edittype : "select",
						editoptions : {
							value : ":;BINARY:BINARY;BOOLEAN:BOOLEAN;EMBEDDED:EMBEDDED;EMBEDDEDLIST:EMBEDDEDLIST;EMBEDDEDMAP:EMBEDDEDMAP;EMBEDDEDSET:EMBEDDEDSET;FLOAT:FLOAT;DATE:DATE;DOUBLE:DOUBLE;INTEGER:INTEGER;LINK:LINK;LINKLIST:LINKLIST;LINKMAP:LINKMAP;LINKSET:LINKSET;LONG:LONG;SHORT:SHORT;STRING:STRING"
						},
						editable : true
					}, {
						name : 'linkedClass',
						index : 'linkedClass',
						width : 150,
						edittype : "select",
						editoptions : {
							value : classEnumeration
						},
						editable : true
					}, {
						name : 'mandatory',
						index : 'mandatory',
						width : 90,
						formatter : 'checkbox',
						edittype : 'checkbox',
						editable : true
					}, {
						name : 'notNull',
						index : 'notNull',
						width : 80,
						formatter : 'checkbox',
						edittype : 'checkbox',
						editable : true
					}, {
						name : 'min',
						index : 'min',
						width : 120,
						editable : true
					}, {
						name : 'max',
						index : 'max',
						width : 120,
						editable : true
					} ], null, {
				editurl : getStudioURL('classProperties'),
				sortname : 'id'
			});

	$("#addProperty").click(function() {
		jQuery("#classProperties").jqGrid('editGridRow', "new", {
			height : 320,
			reloadAfterSubmit : false,
			closeOnEscape : true,
			closeAfterAdd : true,
			editData : [ selectedClassName ],
			afterSubmit : function(response, postdata) {
				jQuery("#output").val(response.responseText);
				return true;
			}
		});
	});
	$("#deleteProperty").click(
			function() {
				var selectedRow = jQuery("#classProperties").jqGrid(
						'getGridParam', 'selrow');
				if (selectedRow != null) {
					var propName = jQuery('#classProperties').jqGrid(
							'getRowData', selectedRow)["name"];
					jQuery("#classProperties").jqGrid(
							'delGridRow',
							selectedRow,
							{
								reloadAfterSubmit : false,
								closeOnEscape : true,
								delData : [ selectedClassName, propName ],
								afterSubmit : function(response, postdata) {
									jQuery("#output")
											.val(response.responseText);
									return [ true, response.responseText ];
								}
							});
				} else
					alert("Please select the property to delete!");
			});

	fillDynaTable($('#databaseClasses'), "Classes", [ 'id', 'name', 'clusters',
			'defaultCluster', 'records' ], [ {
		name : 'id',
		index : 'id',
		width : 30
	}, {
		name : 'name',
		index : 'name',
		width : 280,
		editable : true,
		editrules : {
			required : true
		},
		formoptions : {
			elmprefix : '(*)'
		}
	}, {
		name : 'clusters',
		index : 'clusters',
		width : 80,
		editable : false
	}, {
		name : 'defaultCluster',
		index : 'defaultCluster',
		width : 80,
		editable : false
	}, {
		name : 'records',
		index : 'records',
		width : 150,
		editable : false
	} ], database['classes'], {
		sortname : 'id',
		editurl : getStudioURL('classes'),
		onSelectRow : function(classRowNum) {
			selectedClassName = databaseInfo['classes'][classRowNum - 1].name;
			fillDynaTableRows($('#classProperties'),
					databaseInfo['classes'][classRowNum - 1]['properties']);
		}
	});

	$("#addClass").click(function() {
		jQuery("#databaseClasses").jqGrid('editGridRow', "new", {
			height : 320,
			reloadAfterSubmit : false,
			closeOnEscape : true,
			closeAfterAdd : true,
			editData : [ selectedClassName ],
			afterSubmit : function(response, postdata) {
				jQuery("#output").val(response.responseText);
				return true;
			}
		});
	});
	$("#deleteClass").click(
			function() {
				var selectedRow = jQuery("#databaseClasses").jqGrid(
						'getGridParam', 'selrow');
				if (selectedRow != null) {
					jQuery("#databaseClasses").jqGrid(
							'delGridRow',
							selectedRow,
							{
								reloadAfterSubmit : false,
								closeOnEscape : true,
								delData : [ selectedClassName ],
								afterSubmit : function(response, postdata) {
									jQuery("#output")
											.val(response.responseText);
									return [ true, response.responseText ];
								}
							});
				} else
					alert("Please select the class to delete!");
			});

	fillDynaTable($('#databaseConfig'), "Configuration", [ 'name', 'value' ],
			null, database['config'].values, {
				sortname : 'name'
			});

	fillDynaTable($('#databaseConfigProperties'), "Configuration properties", [
			'name', 'value' ], null, database['config'].properties, {
		sortname : 'name'
	});
}

function queryResponse(data) {
	var result = data["result"];
	var schema = data["schema"];

	jQuery("#output").val(
			"Query executed in " + stopTimer() + " sec. Returned "
					+ result.length + " record(s)");

	// CREATE COLUMN NAMES
	var columnNames = buildColumnNames(result);

	// CREATE COLUMN MODEL
	var columnModel = new Array();

	columnModel.push( {
		"name" : "_id",
		"index" : "_id",
		"width" : 30,
		"classes" : "cell_readonly"
	});
	columnModel.push( {
		"name" : "_ver",
		"index" : "_ver",
		"width" : 30,
		"classes" : "cell_readonly"
	});
	columnModel.push( {
		"name" : "_class",
		"index" : "_class",
		"width" : 30,
		"classes" : "cell_readonly"
	});
	columnModel.push( {
		"name" : "_class",
		"index" : "_className",
		edittype : "select",
		editoptions : {
			value : classEnumeration
		},
		hidden : true,
		editable : true,
		editrules : {
			edithidden : true
		}
	});

	var formatter;
	var editFormatter;
	var editOptions;

	for (col in columnNames) {
		editOptions = null;

		if (schema && schema.properties && schema.properties[columnNames[col]]) {
			var type = schema.properties[columnNames[col]].type;
			switch (type) {
			case 'STRING':
				formatter = "text";
				break;
			case 'BOOLEAN':
				formatter = "checkbox";
				editOptions = {
					value : "True:False"
				};
				break;
			case 'EMBEDDED':
				formatter = "embeddedFormatter";
				break;
			case 'LINK':
				formatter = "linkFormatter";
				break;
			case 'EMBEDDEDLIST':
				formatter = "embeddedListFormatter";
				break;
			case 'LINKLIST':
				formatter = "linkListFormatter";
				break;
			case 'EMBEDDEDSET':
				formatter = "embeddedSetFormatter";
				break;
			case 'LINKSET':
				formatter = "linkSetFormatter";
				break;
			default:
				formatter = "text";
			}
		} else
			formatter = "text";

		editFormatter = formatter;

		if (col.charAt(0) !== '_') {
			columnModel.push( {
				"name" : columnNames[col],
				"editable" : true,
				"index" : columnNames[col],
				"width" : 80,
				"formatter" : formatter,
				"edittype" : editFormatter,
				"editoptions" : editOptions
			});
		}
	}

	var lastsel;

	jQuery($('#queryResultTable')).jqGrid('GridUnload');
	fillDynaTable($('#queryResultTable'), "Resultset", columnNames,
			columnModel, result, {
				sortname : 'id',
				width : 400,
				height : 330,
				editurl : getStudioURL('document'),
				onSelectRow : function(id) {
					if (id && id !== lastsel) {
						jQuery('#queryResultTable').jqGrid('restoreRow',
								lastsel);
						lastsel = id;
					}

					var recId = jQuery('#queryResultTable').jqGrid(
							'getRowData', id)["_id"];
					jQuery('#queryResultTable').jqGrid('editRow', id, true,
							null, function(response, postdata) {
								jQuery("#output").val(response.responseText);
								return true;
							}, getStudioURL('document'), [ recId ]);
				}
			});

	$("#newRecord").click(function() {
		jQuery("#queryResultTable").jqGrid('editGridRow', "new", {
			height : 280,
			reloadAfterSubmit : false,
			closeAfterAdd : true,
			closeOnEscape : true,
			afterSubmit : function(response, postdata) {
				jQuery("#output").val(response.responseText);
				return true;
			}
		});
	});
	$("#deleteRecord").click(
			function() {
				var selectedRow = jQuery("#queryResultTable").jqGrid(
						'getGridParam', 'selrow');
				if (selectedRow != null) {
					var recId = jQuery('#queryResultTable').jqGrid(
							'getRowData', selectedRow)["_id"];
					jQuery("#queryResultTable").jqGrid(
							'delGridRow',
							selectedRow,
							{
								reloadAfterSubmit : false,
								closeAfterDelete : true,
								closeOnEscape : true,
								delData : [ recId ],
								afterSubmit : function(response, postdata) {
									jQuery("#output")
											.val(response.responseText);
									return [ true, response.responseText ];
								}
							});
				} else
					alert("Please Select Row to delete!");
			});
}

function executeQuery() {
	startTimer();

	jQuery("#queryText").val(jQuery.trim($('#queryText').val()));

	executeJSONRequest($('#server').val() + '/query/' + $('#database').val()
			+ '/sql/' + $('#limit').val(), queryResponse, jQuery("#queryText")
			.val(), 'POST');
}

function executeCommand() {
	startTimer();

	jQuery("#commandText").val(jQuery.trim($('#commandText').val()));

	$.ajax( {
		type : 'POST',
		url : $('#server').val() + '/command/sql/' + $('#database').val() + '/'
				+ $('#commandText').val() + '/' + $('#limit').val(),
		success : function(msg) {
			jQuery("#commandOutput").val(msg);
			jQuery("#output").val(
					"Command executed in " + stopTimer() + " sec.");
		},
		data : jQuery("#commandOutput").val(),
		error : function(msg) {
			jQuery("#commandOutput").val("");
			jQuery("#output").val("Command response: " + msg);
		}
	});
}

function executeRawCommand() {
	startTimer();

	var req = $('#server').val() + '/' + $('#rawOperation').val() + '/'
			+ $('#rawDatabase').val() + '/' + $('#rawArgs').val();

	$.ajax( {
		type : $('#rawMethod').val(),
		url : req,
		success : function(msg) {
			jQuery("#rawOutput").val(msg);
			jQuery("#output").val(
					"Raw command executed in " + stopTimer() + " sec.");
		},
		data : jQuery("#rawOutput").val(),
		error : function(msg) {
			jQuery("#rawOutput").val("");
			jQuery("#output").val("Command response: " + msg);
		}
	});
}

function startTimer() {
	startTime = new Date().getTime();
}

function stopTimer() {
	return ((new Date().getTime() - startTime) / 1000);
}

function clearResultset() {
	jQuery("#queryResultTable").jqGrid('clearGridData');
}

function getStudioURL(context) {
	return $('#server').val() + '/studio/' + context + '/'
			+ $('#database').val()
}

function askServerInfo() {
	executeJSONRequest($('#server').val() + '/server', function(server) {
		fillStaticTable($('#serverConnections'),
				[ 'Id', 'Remote Client', 'Database', 'User', 'Protocol',
						'Total requests', 'Command info', 'Command detail',
						'Last Command When', 'Last command info',
						'Last command detail', 'Last execution time',
						'Total working time', 'Connected since' ],
				server['connections']);
		fillStaticTable($('#serverDbs'), [ 'Database', 'User', 'Status',
				'Storage' ], server['dbs']);
		fillStaticTable($('#serverStorages'), [ 'Name', 'Type', 'Path',
				'Active users' ], server['storages']);
		fillStaticTable($('#serverConfigProperties'), [ 'Name', 'Value' ],
				server['properties']);

		fillStaticTable($('#serverProfilerStats'), [ 'Name', 'Value' ],
				server['profiler']['stats']);
		fillStaticTable($('#serverProfilerChronos'), [ 'Name', 'Total',
				'Average Elapsed (ms)', 'Min Elapsed (ms)', 'Max Elapsed (ms)',
				'Last Elapsed (ms)', 'Total Elapsed (ms)' ],
				server['profiler']['chronos']);
	});
}

function askDatabaseInfo() {
	executeJSONRequest(
			$('#server').val() + '/database/' + $('#database').val(),
			showDatabaseInfo);
}

function clear(component) {
	$('#' + component).val("");
}

jQuery(document).ready(
		function() {
			jQuery(document).ajaxError(function(event, request, settings, err) {
				jQuery("#output").val("Error: " + request.responseText);
			});

			$("#tabs-main").hide();
			$("#buttonDisconnect").hide();

			$("#tabs-main").tabs();
			$("#tabs-db").tabs();
			$("#tabs-security").tabs();
			$("#tabs-server").tabs();

			$('#server').change(function(objEvent) {
				var s = $('#server').val();
				if (s.charAt(s.length - 1) == "/")
					$('#server').val(s.substring(0, s.length - 1));

				$('#rawServer').html($('#server').val() + "/");
			});

			if (document.location.href
					.charAt(document.location.href.length - 1) == "/")
				$('#server').val(
						document.location.href.substring(0,
								document.location.href.length - 1));
			else
				$('#server').val(document.location.href);

			jQuery("#queryText").val(jQuery.trim(jQuery("#queryText").val()));
			jQuery("#commandText").val(
					jQuery.trim(jQuery("#commandText").val()));
			jQuery("#output").val(jQuery.trim(jQuery("#output").val()));
		});
