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
var ODocumentViewInstances = {};

function ODocumentView(name, component, doc, options) {
	this.name = name;
	this.database = null;
	this.componentId = component;
	this.component = $('#' + component);
	this.doc = null;
	this.fieldNum = 0;

	ODocumentViewInstances[name] = this;

	this.settings = {
		editable : true,

		ridLabelStyleClass : "odocumentview_rid_label",
		ridValueStyleClass : "odocumentview_rid_value",

		classLabelStyleClass : "odocumentview_class_label",
		classValueStyleClass : "odocumentview_class_value",

		versionLabelStyleClass : "odocumentview_version_label",
		versionValueStyleClass : "odocumentview_version_value",

		typeLabelStyleClass : "odocumentview_type_label",
		typeValueStyleClass : "odocumentview_type_value",

		fieldColumnName : "Field",
		fieldStyleClass : "odocumentview_field_label",
		valueColumnName : "Value",
		valueStyleClass : "odocumentview_field_value",

		valueHtmlComponentType : "textarea",
		valueHtmlComponentScript : function(obj, fieldId, fieldName) {
			$('#doc_' + fieldId + '_value').autoResize(
					obj.settings.valueHtmlComponentSettings);
		},
		valueHtmlComponentSettings : {
			// On resize:
			onResize : function() {
				$(this).css({
					opacity : 0.8
				});
			},
			// After resize:
			animateCallback : function() {
				$(this).css({
					opacity : 1
				});
			},
			// Quite slow animation:
			animateDuration : 300,
			// More extra space:
			extraSpace : 40
		},

		log : null
	};

	if (options != null) {
		// OVERWRITE VALUES
		for (i in options) {
			this.settings[i] = options[i];
		}
	}

	this.types = [ "binary", "boolean", "embedded", "embeddedlist",
			"embeddedmap", "embeddedset", "decimal", "float", "date",
			"datetime", "double", "integer", "link", "linklist", "linkmap",
			"linkset", "long", "short", "string" ];

	ODocumentView.prototype.render = function(doc, database) {
		if (database != null)
			this.database = database;

		if (doc != null && this.database != null) {
			if (typeof doc == "string")
				// LOAD THE RECORD BY RID
				this.doc = database.load(doc);
			else
				this.doc = doc;
		}

		var script = "<script>";

		// BEGIN COMMANDS
		component = "<div id='" + this.componentId + "_header' class='row-fluid'>";
		// END COMMANDS

		// BEGIN RECORD ATTRIBUTES
		component += "<div class='row-fluid'>";
		component += "<div class='span6'><form class='form-inline'>";

		var fieldValue
		if (this.doc != null)
			fieldValue = this.doc['@class'];

		component += "<label>@class&nbsp;</label>"
				+ generateClassSelect("doc__class", fieldValue);

		var currentClass = orientServer.getClass(fieldValue);

		var fieldValue = "-1:-1";
		if (this.doc != null && this.doc['@rid'])
			fieldValue = this.doc['@rid'].substring(1);

		component += "<label>&nbsp;@rid&nbsp;</label><input id='doc__rid' style='width: 60px;' value='"
				+ fieldValue + "'/>";

		if (this.doc != null)
			fieldValue = this.doc['@version'];
		else
			fieldValue = 0;

		component += "<label>&nbsp;@version&nbsp;</label><input id='doc__version' style='width: 40px;' disabled value='"
				+ fieldValue + "'/>";

		component += "</form></div>";
		component += "<div class='span6 btn-group'>"

			+ this.generateButton('doc_graph', 'Graph', 'icon-picture',
					"btn", "ODocumentView.graph('" + this.name + "')")

			+ this.generateButton('doc_create', 'Create', 'icon-plus',
					"btn", "ODocumentView.create('" + this.name + "')")

			+ this.generateButton('doc_delete', 'Delete', 'icon-remove',
					"btn", "ODocumentView.remove('" + this.name + "')")

			+ this.generateButton('doc_reload', 'Reload', 'icon-refresh',
					"btn", "ODocumentView.reload('" + this.name + "')")

			+ this.generateButton('doc_copy', 'Copy', 'icon-lock', "btn",
					"ODocumentView.copy('" + this.name + "')")

			+ this.generateButton('doc_undo', 'Undo', 'icon.repeat', "btn",
					"ODocumentView.undo('" + this.name + "')")

			+ this.generateButton('doc_clear', 'Clear', 'icon-trash',
					"btn", "ODocumentView.clear('" + this.name + "')");
	component += "</div></div>";

				"</div></div>";

		// FIELD BIG BLOCK
		component += "<div class='row-fluid'><div class='well'><div class='row-fluid'><div class='span12'><div class='row-fluid'>";

		// HEADER
		component += "<div class='span2'>"
				+ this.settings.fieldColumnName + "</div>"
				+ "<div class='span10'>"
				+ this.settings.valueColumnName + "</div>";

		component += "</div></div>";

		// BEGIN FIELDS
		component += "<div class='span12' id='" + this.componentId
				+ "_fields'>";

		var fieldValue;
		this.fieldNum = 0;
		if (this.doc != null)
			for (fieldName in this.doc) {
				if (fieldName.charAt(0) == '@')
					continue;

				fieldValue = this.doc[fieldName];
				var fieldType = "string";
				if (currentClass)
					for (p in currentClass.properties) {
						if (currentClass.properties[p].name == fieldName) {
							fieldType = currentClass.properties[p].type;
							break;
						}
					}
				component += this.renderRow(fieldName, fieldValue, fieldType);
			}
		component += "</div>";
		// END FIELDS

		// BEGIN ADD BUTTON
		component += "<div class='offset10 span2'>"
				+ this.generateButton('doc_addField', 'Add Field', 'icon-plus',
						"btn", "ODocumentView.addField('" + this.name + "')", "rel='tooltip' data-placement='bottom' data-original-title='Adds a field to the document'")
				+ "</div>";
		// END ADD BUTTON		

		component += "</div></div><div>"
				+ this.generateButton('doc_save', 'Save', 'icon-ok',
						"btn btn-large btn-primary", "ODocumentView.save('"
								+ this.name + "')") + "</div>";

		component += "</div></div></div></div>";
		script += "</script>";

		this.component.html(component + script);

		if (this.settings.editable && this.doc != null) {
			var i = 0;
			for (fieldName in this.doc) {
				if (fieldName.charAt(0) == '@')
					continue;

				this.settings.valueHtmlComponentScript(this, i++, fieldName);
			}
		}
	}

	ODocumentView.prototype.renderRow = function(fieldName, fieldValue,
			fieldType) {

		if (fieldType)
			fieldType = fieldType.toLowerCase();

		component = "<div class='row-fluid' id='doc_" + this.fieldNum
				+ "'>";

		// BEGIN FIELD LABEL
		component += "<div class='span2'><input id='doc_"
				+ this.fieldNum
				+ "_label' class='input-small' value='";
		component += fieldName;
		component += "'/></div>";
		// END FIELD LABEL

		// BEGIN FIELD VALUE
		component += "<div class='span8'>";
		if (this.settings.editable) {
			component += "<" + this.settings.valueHtmlComponentType
					+ " id='doc_" + this.fieldNum + "_value' ";
			if (this.settings.valueStyleClass != null)
				component += " class='" + this.settings.valueStyleClass + "'";
			component += ">";
		}
		// END FIELD VALUE

		if (fieldValue == null)
			component += 'null';
		else if (fieldValue instanceof Array) {
			component += '[';
			for (v in fieldValue) {
				if (v > 0)
					component += ', ';

				if (typeof fieldValue[v] == 'object' && fieldValue[v]['@rid'])
					component += ('#' + fieldValue[v]['@rid']);
				else
					component += fieldValue[v];
			}
			component += ']';
		} else if (typeof fieldValue == 'object' && fieldValue['@rid']) {
			component += ('#' + fieldValue['@rid']);
		} else {
			component += fieldValue;
		}

		if (this.settings.editable) {
			component += "</" + this.settings.valueHtmlComponentType + ">";
		}
		component += "</div>";
		// END FIELD VALUE

		// BEGIN FIELD TYPE + REMOVE
		component += "<div class='span2'>";
		component += "<select id='doc_" + this.fieldNum + "_type' class='"
				+ this.settings.typeValueStyleClass + "'>";
		for (i in this.types) {
			var t = this.types[i];
			component += "<option";
			if (t == fieldType)
				component += " selected = 'yes'";
			component += ">" + t + "</option>";
		}
		component += "</select>";

		component += this.generateButton('doc_' + this.fieldNum + '_remove',
				'', 'icon-trash', null, "ODocumentView.removeField('doc_"
						+ this.fieldNum + "')", "rel='tooltip' data-placement='bottom' data-original-title='Removes this field from the document'");
		component += "</div>";
		// END FIELD TYPE + REMOVE

		component += "</div>";

		this.fieldNum++;

		return component;
	}

	ODocumentView.prototype.generateButton = function(id, label, image,
			styleClass, onClick, additionalTags) {
		var out = "<button id='" + id + "' onClick=\"javascript:" + onClick
				+ "\"";
		if (styleClass)
			out += " class='" + styleClass + "'";
		if( additionalTags)
			out += additionalTags;
		out += ">";
		if (image != null)
			out += "<i alt='" + label + "' class='" + image + "'/> ";
		out += label;
		out += "</button>";
		return out;
	}

	ODocumentView.prototype.save = function() {
		var fieldName;
		var fieldValue;
		var fieldType;
		var object = {};

		object['@rid'] = $('#doc__rid').val();
		object['@class'] = $('#doc__class').val();
		object['@version'] = parseInt($('#doc__version').val());

		for ( var i = 0; i < this.fieldNum; ++i) {
			fieldName = $('#doc_' + i + '_label').val();

			if (fieldName != null) {
				fieldValue = $('#doc_' + i + '_value').val().trim();
				fieldType = $('#doc_' + i + '_type').val();

				if (fieldType == 'linkset' || fieldType == 'linklist'
						|| fieldType == 'embeddedset'
						|| fieldType == 'embeddedlist') {
					object[fieldName] = [];
					if (fieldValue.length > 0) {
						fieldValue = fieldValue.substring(1,
								fieldValue.length - 1);
						fieldValue = fieldValue.split(",");
						for (fieldIndex in fieldValue) {
							object[fieldName].push($
									.trim(fieldValue[fieldIndex]));
						}
					}
				} else {
					if (isNaN(fieldValue))
						object[fieldName] = fieldValue;
					else {
						if (fieldValue.indexOf('.') > -1)
							object[fieldName] = parseFloat(fieldValue);
						else
							object[fieldName] = parseInt(fieldValue);
					}
				}

			}
		}

		var result = this.database.save(object);

		if (result.charAt(0) == '#')
			$('#doc__rid').val(result.substring(0));

		if (this.settings.log != null) {
			var msg = this.database.getErrorMessage();
			if (msg == null)
				msg = this.database.getCommandResponse();
			this.settings.log(msg);
		}

		return object;
	}

	ODocumentView.prototype.create = function(className) {
		$('#doc__rid').val("-1:-1");
		$('#doc__version').val("0");

		if (className == null)
			className = $('#doc__class').val();

		if (className == null)
			return;

		var selectedClass = null;
		for (cls in databaseInfo['classes']) {
			if (databaseInfo['classes'][cls].name == className) {
				selectedClass = databaseInfo['classes'][cls];
				$('#doc__class').val(className);
				break;
			}
		}

		if (selectedClass == null)
			return;

		var component = $('#' + this.componentId + "_fields");
		component.empty();

		// SET THE DECLARED FIELDS FOUND INTO THE SCHEMA
		for (p in selectedClass.properties) {
			found = false;

			for ( var i = 0; i < this.fieldNum; ++i) {
				fieldName = $('#doc_' + i + '_label').val();
				if (fieldName != null
						&& selectedClass.properties[p].name == fieldName) {
					found = true;
					break;
				}
			}

			if (!found)
				component.append(this.renderRow(
						selectedClass.properties[p].name, "",
						selectedClass.properties[p].type));
		}
	}

	ODocumentView.prototype.undo = function() {
		this.render();
	}

	ODocumentView.prototype.reload = function() {
		this.doc = this.database.load($('#doc__rid').val());
		this.render();
	}

	ODocumentView.prototype.copy = function() {
		$('#doc__rid').val("-1:-1");
	}

	ODocumentView.prototype.remove = function() {
		var rid = $('#doc__rid').val();

		this.database.remove(rid);

		if (this.settings.log != null) {
			var msg = this.database.getErrorMessage();
			if (msg == null)
				msg = this.database.getCommandResponse();
			this.settings.log(msg);
		}

		return rid;
	}

	ODocumentView.prototype.graph = function() {
		if (selectedObject != null)
			displayGraph(selectedObject);
	}

	ODocumentView.prototype.clear = function() {
		var i = 0;
		var fieldName;

		while (true) {
			fieldName = $('#doc_' + i + '_label').val();

			if (fieldName != null) {
				$('#doc_' + i + '_value').val("");
				i++;
			} else
				break;
		}
	}
	ODocumentView.addField = function(instanceName) {
		var instance = ODocumentViewInstances[instanceName];
		$('#' + instance.getComponentId() + "_fields").append(
				instance.renderRow("", "", "string"));
	}

	ODocumentView.removeField = function(id) {
		$('#' + id).remove();
	}

	ODocumentView.prototype.getComponentId = function() {
		return this.componentId;
	}

	ODocumentView.create = function(instanceName) {
		return ODocumentViewInstances[instanceName].create();
	}

	ODocumentView.save = function(instanceName) {
		return ODocumentViewInstances[instanceName].save();
	}

	ODocumentView.remove = function(instanceName) {
		return ODocumentViewInstances[instanceName].remove();
	}

	ODocumentView.clear = function(instanceName) {
		return ODocumentViewInstances[instanceName].clear();
	}

	ODocumentView.graph = function(instanceName) {
		return ODocumentViewInstances[instanceName].graph();
	}

	ODocumentView.undo = function(instanceName) {
		return ODocumentViewInstances[instanceName].undo();
	}
	ODocumentView.reload = function(instanceName) {
		return ODocumentViewInstances[instanceName].reload();
	}
	ODocumentView.copy = function(instanceName) {
		return ODocumentViewInstances[instanceName].copy();
	}

	if (doc != null)
		this.render(doc);
}
