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

function defaultSimpleRequestError(msg) {
	$("#output").text("Command response: " + msg);
}

function executeSimpleRequest(iRequest, iSuccessCallback, iErrorCallback) {
	if (!iErrorCallback)
		iErrorCallback = defaultSimpleRequestError;

	$.ajax({
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
			columnModel.push({
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

	$(iTable).jqGrid(config);

	if (iToolBar) {
		if (navBar)
			$(iTable).jqGrid("navGrid", navBar);

		$(iTable).jqGrid('filterToolbar', {
			stringResult : true,
			searchOnEnter : false
		});
	}

	fillDynaTableRows(iTable, iData);
}

function fillDynaTableRows(iTable, iData) {
	$(iTable).jqGrid('clearGridData');

	if (iData)
		for ( var i = 0; i <= iData.length; i++)
			$(iTable).jqGrid('addRowData', i + 1, iData[i]);
}

function fillStaticTable(iTable, iColumns, iData) {
	$(iTable).text("");

	var line = "";
	for (col in iColumns) {
		line += '<td style=\'font-size: 8pt;\'><b>' + iColumns[col]
				+ '</b></td>';
	}
	$(iTable).append('<tr>' + line + '</tr>');

	for (row in iData) {
		var values = iData[row];

		var line = ""
		var i = 0;
		for (col in values) {
			if (i++ >= iColumns.length)
				break;

			line += '<td style=\'font-size: 8pt;\'>' + values[col] + '</td>';
		}
		$(iTable).append('<tr>' + line + '</tr>');
	}

	$("#output").text("Command executed");
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

function dynaFormatter(cellvalue, options, rowObject) {
	if (typeof cellvalue == 'string' && cellvalue.charAt(0) == '#'
			&& cellvalue.indexOf(':') > -1) {
		// LINK
		return linkFormatter(cellvalue, options, rowObject);
	}
	return cellvalue;
}

function dynaUnformatter(cellvalue, options, rowObject) {
	return cellvalue;
}
function classFormatter(cellvalue, options, rowObject) {
	return "<a onclick=\"openClass('" + cellvalue + "');\" class='className'>"
			+ cellvalue + "</a>";
}

function linkFormatter(cellvalue, options, rowObject) {
	return "<a onclick=\"openLink('" + cellvalue + "');\" class='link'>"
			+ cellvalue + "</a>";
}
function linkUnformatter(cellvalue, options) {
	if (cellvalue)
		return cellvalue.split(" ")[0];

	return "";
}
function linksFormatter(cellvalue, options, rowObject) {
	if (typeof cellvalue == 'string') {
		cellvalue = cellvalue.substring(1, cellvalue.length - 1);
		if (cellvalue.length == 0)
			return "[]";
		cellvalue = cellvalue.split(',');
	}

	var buffer = "[";
	for (i in cellvalue) {
		if (buffer.length > 1)
			buffer += ",";
		buffer += linkFormatter(cellvalue[i]);
	}
	buffer += "]";
	return buffer;
}
function linksUnformatter(cellvalue, options, rowObject) {
	var buffer = "[";
	if (cellvalue.length > 2) {
		var entries = cellvalue.substring(1, cellvalue.length - 1).split(',');
		for (i in entries) {
			if (buffer.length > 1)
				buffer += ",";
			buffer += entries[i].split(' ')[0];
		}
	}
	buffer += "]";
	return buffer;
}
function embeddedFormatter(cellvalue, options, rowObject) {
	return "<img src='images/embedded.png' />";
}

function openClass(clsName) {
	controller.loadFragment("panelDatabase.htm", function() {
		displayClass(clsName);
	});
}
function openLink(rid) {
	displayDocument(rid, orientServer);
}

function displayResultSet(result) {
	if (!result || result.constructor != Array)
		return;

	$("#output").val(
			"Query executed in " + stopTimer() + " sec. Returned "
					+ result.length + " record(s)");

	// CREATE COLUMN NAMES
	var columnNames = buildColumnNames(result);

	// CREATE COLUMN MODEL
	var columnModel = new Array();
	var schema;

	if (result.length > 0) {
		for (cls in databaseInfo['classes']) {
			if (databaseInfo['classes'][cls].name == result[0]["@class"]) {
				schema = databaseInfo['classes'][cls];
				break;
			}
		}
	}

	columnModel.push({
		"name" : "@rid",
		"index" : "@rid",
		"align" : "center",
		"classes" : "cell_readonly",
		"width" : "70px",
		formatter : linkFormatter,
		unformatter : linkUnformatter,
		fixed : true,
		searchoptions : {
			sopt : [ "cn" ]
		}
	});
	columnModel.push({
		"name" : "@version",
		"index" : "@version",
		"classes" : "cell_readonly",
		"width" : "40px",
		fixed : true,
		searchoptions : {
			sopt : [ "cn" ]
		}
	});
	columnModel.push({
		"name" : "@class",
		"index" : "@class",
		"classes" : "cell_readonly",
		"width" : "100px",
		fixed : true,
		formatter : classFormatter,
		searchoptions : {
			sopt : [ "cn" ]
		}
	});

	var formatter;
	var editFormatter;
	var editOptions;

	for (col in columnNames) {
		editOptions = null;
		unformatter = null;

		var type = null;

		if (schema && columnNames[col].charAt(0) != '@') {

			for (p in schema.properties) {
				if (schema.properties[p].name == columnNames[col]) {
					type = schema.properties[p].type;
					break;
				}
			}

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
				formatter = linksFormatter;
				unformatter = linksUnformatter;
				break;
			case 'EMBEDDEDSET':
				formatter = "embeddedSetFormatter";
				break;
			case 'LINKSET':
				formatter = linksFormatter;
				unformatter = linksUnformatter;
				break;
			}
		}

		if (!type) {
			// UNKNOWN: USE DYNAMIC FORMATTER
			formatter = dynaFormatter;
			unformatter = dynaUnformatter;
		}

		editFormatter = formatter;

		if (col.charAt(0) !== '@') {
			columnModel.push({
				name : columnNames[col],
				editable : true,
				index : columnNames[col],
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

	$($('#queryResultTable')).jqGrid('GridUnload');
	fillDynaTable($('#queryResultTable'), "Resultset", columnNames,
			columnModel, result, {
				sortname : '@rid',
				height : 300,
				editurl : getStudioURL('document'),
				onSelectRow : function(id) {
					if (id && id !== lastsel) {
						$('#queryResultTable').jqGrid('restoreRow', lastsel);
						lastsel = id;
					}

					var recId = getRid(id);

					$('#queryResultTable').jqGrid('editRow', id, true, null,
							function(response, postdata) {
								$("#output").val(response.responseText);
								return true;
							}, getStudioURL('document'), [ recId ]);
				}
			}, true);

	$("#newRecord").click(function() {
		$("#queryResultTable").jqGrid('editGridRow', "new", {
			height : 280,
			reloadAfterSubmit : false,
			closeAfterAdd : true,
			closeOnEscape : true,
			afterSubmit : function(response, postdata) {
				$("#output").val(response.responseText);
				return true;
			}
		});
	});
	$("#deleteRecord").click(
			function() {
				var selectedRow = $("#queryResultTable").jqGrid('getGridParam',
						'selrow');
				if (selectedRow != null) {
					var recId = getRid(selectedRow);
					$("#queryResultTable").jqGrid('delGridRow', selectedRow, {
						reloadAfterSubmit : false,
						closeAfterDelete : true,
						closeOnEscape : true,
						delData : [ recId ],
						afterSubmit : function(response, postdata) {
							$("#output").val(response.responseText);
							return [ true, response.responseText ];
						}
					});
				} else
					alert("Please Select Row to delete!");
			});

}

function getRid(id) {
	var obj = $('#queryResultTable').jqGrid('getRowData', id);
	if (!obj)
		return null;

	var recId = obj["@rid"];
	var begin = recId.indexOf('>#');
	if (begin > -1) {
		var end = recId.indexOf('<', begin);
		recId = recId.substring(begin + 1, end);
	}

	selectedObject = recId;
	return recId;
}