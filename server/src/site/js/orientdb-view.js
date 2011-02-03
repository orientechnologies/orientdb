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

		styleClass : "odocumentview",
		showRid : true,
		ridLabelStyleClass : "odocumentview_header_label",
		ridValueStyleClass : "odocumentview_header_value",

		showClass : true,
		classLabelStyleClass : "odocumentview_header_label",
		classValueStyleClass : "odocumentview_header_value",

		showVersion : true,
		versionLabelStyleClass : "odocumentview_header_label",
		versionValueStyleClass : "odocumentview_header_value",

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

	ODocumentView.prototype.render = function(doc, database) {
		if (doc != null)
			this.doc = doc;

		if (database != null)
			this.database = database;

		var component = "<table class='" + this.settings.styleClass
				+ "' width='100%'>";

		var script = "<script>";

		// HEADER, BROWSE RESERVED FIELDS (@RID, @CLASS, @VERSION)
		component += "<tr><td><table id='" + this.componentId
				+ "_header' width='100%'><tr>";

		if (this.settings.showClass) {
			fieldValue = this.doc['@class'];

			classes = "<select id='doc__class' class='"
					+ this.settings.classValueStyleClass + "'>";
			for (cls in databaseInfo['classes']) {
				classes += "<option";
				if (databaseInfo['classes'][cls].name == fieldValue)
					classes += " selected = 'yes'";
				classes += ">" + databaseInfo['classes'][cls].name
						+ "</option>";
			}
			classes += "</select>";

			if (fieldValue != null)
				component += "<td class='" + this.settings.classLabelStyleClass
						+ "'>@class</td><td class='"
						+ this.settings.classValueStyleClass + "'>" + classes
						+ "</td>";
		}

		component += "<td align='left'>"
				+ this.generateButton('doc_create', 'Create ', 'add.png', null,
						"ODocumentView.create('" + this.name + "')") + "</td>";

		component += "<td width='200'></td>";

		var fieldValue;
		if (this.settings.showRid) {
			fieldValue = this.doc['@rid'];
			if (fieldValue != null)
				component += "<td class='" + this.settings.ridLabelStyleClass
						+ "'>@rid</td><td class='"
						+ this.settings.ridValueStyleClass
						+ "'><input id='doc__rid' class='"
						+ this.settings.ridValueStyleClass
						+ "' disabled value='" + fieldValue + "'/></td>";
		}

		if (this.settings.showVersion) {
			fieldValue = this.doc['@version'];
			if (fieldValue != null)
				component += "<td class='"
						+ this.settings.versionLabelStyleClass
						+ "'>@version</td><td class='"
						+ this.settings.versionValueStyleClass
						+ "'><input id='doc__version' class='"
						+ this.settings.versionValueStyleClass
						+ "'disabled value='" + fieldValue + "'/></td>";
		}

		component += "</tr></table></td></tr>";

		component += "<tr><td><table id='" + this.componentId
				+ "_fields' width='100%'><th>" + this.settings.fieldColumnName
				+ "</th><th>" + this.settings.valueColumnName + "</th>";
		var fieldValue;
		this.fieldNum = 0;
		for (fieldName in this.doc) {
			if (fieldName.charAt(0) == '@')
				continue;

			fieldValue = this.doc[fieldName];
			component += this.renderRow(fieldName, fieldValue);
		}

		component += "</table></td></tr>";

		component += "<td align='right'>"
				+ this.generateButton('doc_addField', 'Add Field', 'add.png',
						null, "ODocumentView.addField('" + this.name + "')")
				+ "</td></tr><tr>";

		component += "<td align='left'>"
				+ this.generateButton('doc_save', 'Save ', 'save.png', null,
						"ODocumentView.save('" + this.name + "')") + "</td>";

		component += "</tr></table>";
		script += "</script>";

		this.component.html(component + script);

		if (this.settings.editable) {
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
		component = "<tr id='doc_" + this.fieldNum + "'><td class='"
				+ this.settings.fieldStyleClass + "'><input id='doc_"
				+ this.fieldNum + "_label' class='"
				+ this.settings.fieldStyleClass + "' value='";
		component += fieldName;
		component += "'/></td><td>";

		if (this.settings.editable) {
			component += "<" + this.settings.valueHtmlComponentType
					+ " id='doc_" + this.fieldNum + "_value' ";
			if (this.settings.valueStyleClass != null)
				component += " class='" + this.settings.valueStyleClass + "'";
			component += ">";
		}

		if (fieldValue == null)
			component += 'null';
		else if (fieldValue instanceof Array) {
			fieldType = "a";
			for (v in fieldValue) {
				if (v > 0)
					component += ', ';

				if (typeof fieldValue[v] == 'object')
					component += ('#' + fieldValue[v]['@rid']);
				else
					component += fieldValue[v];
			}
		} else if (typeof fieldValue == 'object') {
			fieldType = "o";
			component += ('#' + fieldValue['@rid']);
		} else {
			fieldType = "";
			component += fieldValue;
		}

		if (this.settings.editable) {
			component += "</" + this.settings.valueHtmlComponentType + ">";
		}

		component += "<input type='hidden' id='doc_" + this.fieldNum
				+ "_type' value='" + fieldType + "'>";

		component += "</td><td class='" + this.settings.fieldStyleClass + "'>";
		component += this.generateButton('doc_' + this.fieldNum + '_remove',
				'', 'remove.png', null, "ODocumentView.removeField('doc_"
						+ this.fieldNum + "')");
		component += "</td></tr>";

		this.fieldNum++;

		return component;
	}

	ODocumentView.prototype.generateButton = function(id, label, image,
			styleClass, onClick) {
		var out = "<button id='" + id + "' onClick=\"javascript:" + onClick
				+ "\"";
		if (styleClass)
			out += " class='" + styleClass + "'";
		out += ">";
		out += label;
		if (image != null)
			out += "<img border='0' alt='" + label + "' src='www/images/"
					+ image + "'/>";
		out += "</button>";
		return out;
	}

	ODocumentView.prototype.save = function() {
		var i = 0;
		var fieldName;
		var fieldValue;
		var fieldType;
		var object = {};

		object['@rid'] = $('#doc__rid').val();
		object['@class'] = $('#doc__class').val();
		if ($('#doc__version') != null)
			object['@version'] = parseInt($('#doc__version').val());

		while (true) {
			fieldName = $('#doc_' + i + '_label').val();

			if (fieldName != null) {
				fieldValue = $('#doc_' + i + '_value').val();
				fieldType = $('#doc_' + i + '_type').val();

				if (fieldType == 'a') {
					object[fieldName] = [];
					fieldValue = fieldValue.split(",");
					for (fieldIndex in fieldValue) {
						object[fieldName].push(jQuery.trim(fieldValue[fieldIndex]));
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

				i++;
			} else
				break;
		}

		this.database.save(object);

		if (this.settings.log != null) {
			var msg = this.database.getErrorMessage();
			if (msg == null)
				msg = this.database.getCommandResponse();
			this.settings.log(msg);
		}

		return object;
	}

	ODocumentView.prototype.create = function() {
		$('#doc__rid').val("-1:-1");
		if ($('#doc__version') != null)
			$('#doc__version').val("0");
	}

	ODocumentView.addField = function(instanceName) {
		var instance = ODocumentViewInstances[instanceName];
		$('#' + instance.getComponentId() + "_fields").append(
				instance.renderRow("", ""));
	}

	ODocumentView.removeField = function(id) {
		$('#' + id).remove();
	}

	ODocumentView.prototype.getComponentId = function() {
		return this.componentId;
	}

	ODocumentView.create = function(instanceName) {
		var instance = ODocumentViewInstances[instanceName];
		instance.create();
	}

	ODocumentView.save = function(instanceName) {
		var instance = ODocumentViewInstances[instanceName];
		instance.save();
	}

	if (doc != null)
		this.render(doc);
}
