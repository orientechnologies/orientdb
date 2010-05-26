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
	if( !iErrorCallback )
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
		iCustomConfig) {
	var columnModel = iColumnsModel;
	if (!columnModel) {
		var columnModel = new Array();
		for (col in iColumnsNames) {
			columnModel.push( {
				"name" : iColumnsNames[col],
				"index" : iColumnsNames[col]
			});
		}
	}

	var config = {
		datatype : "local",
		autowidth : true,
		multiselect : false,
		viewrecords : true,
		caption : iTitle,
		colNames : iColumnsNames,
		colModel : columnModel
	};

	if (iCustomConfig)
		// MERGE SETTINGS
		for (property in iCustomConfig)
			config[property] = iCustomConfig[property];

	jQuery(iTable).jqGrid(config);

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
	return "<img src='images/link.png' />";
}

function embeddedFormatter(cellvalue, options, rowObject) {
	return "<img src='images/embedded.png' />";
}
