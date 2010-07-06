function orient_connect(url, dbName, callback) {
	executeJSONRequest(url + '/connect/' + dbName, callback);
}

function executeJSONRequest(iRequest, iCallback, iData, iMethod) {
	if (!iMethod)
		iMethod = 'GET';

	$.ajax( {
		type : iMethod,
		url : iRequest,
		success : function(msg) {
			iCallback.apply(this, [ jQuery.parseJSON(msg) ]);
		},
		data : iData,
		error : function(msg) {
			jQuery("#output").text("Command response: " + msg);
		}
	});
}

function defaultSimpleRequestError(msg) {
	jQuery("#output").text("Command response: " + msg);
}

function executeSimpleRequest(iRequest, iSuccessCallback, iErrorCallback) {
	if (!iErrorCallback)
		iErrorCallback = defaultSimpleRequestError;

	$.ajax( {
		type : 'GET',
		url : iRequest,
		success : function(msg) {
			iSuccessCallback.apply(this, [ msg ]);
		},
		error : iErrorCallback
	});
}

function fillDynaTable(iTable, iTitle, iColumnsNames, iColumnsModel, iData,
		iCustomConfig, iToolBar) {
	var columnModel = iColumnsModel;
	if (!columnModel) {
		var columnModel = new Array();
		for (col in iColumnsNames) {
			columnModel.push( {
				name : iColumnsNames[col],
				index : iColumnsNames[col]
			});
		}
	}

	var navBar = $("#" + iTable.attr("id") + "Nav");

	var config = {
		datatype : "local",
		autowidth : true,
		multiselect : false,
		viewrecords : true,
		gridview : true,
		sortname : "_id",
		sortorder : "asc",
		caption : iTitle,
		loadonce : true,
		colNames : iColumnsNames,
		colModel : columnModel
	};

	if (navBar)
		config['pager'] = "#" + iTable.attr("id") + "Nav";

	if (iCustomConfig)
		// MERGE SETTINGS
		for (property in iCustomConfig)
			config[property] = iCustomConfig[property];

	jQuery(iTable).jqGrid(config);

	if (iToolBar) {
		if (navBar)
			jQuery(iTable).jqGrid("navGrid", navBar);

		jQuery(iTable).jqGrid('filterToolbar', {
			stringResult : true,
			searchOnEnter : false
		});
	}

	fillDynaTableRows(iTable, iData);
}

function fillDynaTableRows(iTable, iData) {
	jQuery(iTable).jqGrid('clearGridData');

	if (iData)
		for ( var i = 0; i <= iData.length; i++)
			jQuery(iTable).jqGrid('addRowData', i + 1, iData[i]);
}

function fillStaticTable(iTable, iColumns, iData) {
	$(iTable).text("");

	var line = "";
	for (col in iColumns) {
		line += '<td><b>' + iColumns[col] + '</b></td>';
	}
	$(iTable).append('<tr>' + line + '</tr>');

	for (row in iData) {
		var values = iData[row];

		var line = ""
		var i = 0;
		for (col in values) {
			if (i++ >= iColumns.length)
				break;

			line += '<td>' + values[col] + '</td>';
		}
		$(iTable).append('<tr>' + line + '</tr>');
	}

	jQuery("#output").text("Command executed");
}

function buildColumnNames(table) {
	var columnNames = new Array();

	// CREATE COLUMN NAMES
	for (row = 0; row < table.length; row++) {
		for (col in table[row]) {
			if (!columnNames[col])
				columnNames[col] = col;
		}
	}
	return columnNames;
}

function linkFormatter(cellvalue, options, rowObject) {
	return cellvalue + " <img src='www/images/link.png' onclick=\"openLink('"
			+ cellvalue + "');\" />";
}
function linkUnformatter(cellvalue, options) {
	if (cellvalue)
		return cellvalue.split(" ")[0];

	return "";
}
function embeddedFormatter(cellvalue, options, rowObject) {
	return "<img src='www/images/embedded.png' />";
}

function openLink(cellvalue) {
	alert(cellvalue);
}

function displayResultSet(result, schema) {
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
		"classes" : "cell_readonly",
		searchoptions : {
			sopt : [ "cn" ]
		}
	});
	columnModel.push( {
		"name" : "_ver",
		"index" : "_ver",
		"width" : 30,
		"classes" : "cell_readonly",
		searchoptions : {
			sopt : [ "cn" ]
		}
	});
	columnModel.push( {
		"name" : "_class",
		"index" : "_class",
		"width" : 30,
		"classes" : "cell_readonly",
		searchoptions : {
			sopt : [ "cn" ]
		}
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
		search : false,
		editrules : {
			edithidden : true
		}
	});

	var formatter;
	var editFormatter;
	var editOptions;

	for (col in columnNames) {
		editOptions = null;
		unformatter = null;

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
				formatter = linkFormatter;
				unformatter = linkUnformatter;
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
				name : columnNames[col],
				editable : true,
				index : columnNames[col],
				width : 80,
				formatter : formatter,
				unformat : unformatter,
				// edittype : editFormatter,
				editoptions : editOptions,
				search : true,
				searchoptions : {
					sopt : [ "cn" ]
				}
			});
		}
	}

	var lastsel;

	jQuery($('#queryResultTable')).jqGrid('GridUnload');
	fillDynaTable($('#queryResultTable'), "Resultset", columnNames,
			columnModel, result, {
				sortname : '_id',
				width : 400,
				height : 300,
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
			}, true);

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
