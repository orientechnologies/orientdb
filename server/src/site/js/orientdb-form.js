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

function OForm() {
	this.object = null;
	this.templateMap = {};
	this.fieldTypes = {};

	/**
	 * Binds an object to the current page. When called recursively, prefix
	 * parameter contains the caller object's field.
	 */
	OForm.prototype.object2form = function(obj, prefix, level) {
		if (this.object == null)
			this.object = obj;

		if (prefix == null)
			prefix = "";
		else
			prefix = prefix + "_";

		if (level == null)
			level = 0;

		this.fieldTypes[prefix] = 'o';

		for (field in obj) {
			if (field.charAt(0) == "@")
				continue;

			var value = obj[field];

			var componentName = prefix + field;
			var component = $("#" + componentName);

			if (value instanceof Array)
				this.array2component(value, component, componentName, level);
			else if (typeof value == "object")
				this.object2form(value, prefix + field, level);
			else
				this.value2component(value, component, level);
		}
	}

	/**
	 * Binds an array to a component.
	 * 
	 * @param array
	 *            value to map as array
	 * @param component
	 *            HTML component
	 */
	OForm.prototype.array2component = function(array, component, prefix, level) {
		if (component != null && component.is('table')) {
			componentChild = component.children().last();

			var referenceRow = this.templateMap[prefix];

			if (referenceRow == null) {
				// FIRST TIME: SEARCH THE TEMPLATE
				referenceRow = componentChild.html();
				this.templateMap[prefix] = referenceRow;
			} else
				referenceRow = this.templateMap[prefix];

			component.empty();

			var indexToFind = "_?" + level;
			for (index in array) {
				var row = referenceRow;
				while (row.indexOf(indexToFind) > -1)
					row = row.replace(indexToFind, "_" + index);

				component.append(row);

				var value = array[index];
				if (value != null && typeof value == "object") {
					if (index == 0)
						this.fieldTypes[prefix] = 'o';

					this.object2form(value, prefix + "_" + index, level + 1);
				} else {
					if (index == 0)
						this.fieldTypes[prefix] = 'v';
					this
							.value2component(value, prefix + "_" + index,
									level + 1);
				}
			}
		}
	}

	/**
	 * Binds a generic simple value to a component.
	 * 
	 * @param value
	 *            value to map
	 * @param component
	 *            HTML component
	 */
	OForm.prototype.value2component = function(value, component) {
		if (typeof component == "string") {
			this.fieldTypes[component] = 'v';

			// SEARCH THE COMPONENT
			component = $("#" + component);
			if (component == null) {
				// SEARCH WITH LAST PIECE OF THE NAME
				var lastPiecePos = component.lastIndexOf("_");
				if (lastPiecePos > -1)
					component = $("#" + component.substring(lastPiecePos));
			}
		}

		if (component != null) {
			// SET THE VALUE
			if (component.is('input')) {
				component.val(value);
			} else
				// AS TEXT
				component.text(value);
		}
	}

	/**
	 * Binds back the form's values to the original object.
	 */
	OForm.prototype.form2object = function(obj, prefix, field) {
		if (obj == null)
			obj = this.object;

		if (prefix == null)
			prefix = "";
		else
			prefix = prefix + "_";

		for (field in obj) {
			if (field.charAt(0) == "@")
				continue;

			var value = obj[field];

			var componentName = prefix + field;
			var component = $("#" + componentName);

			if (value instanceof Array)
				this.component2array(obj, component, componentName, field);
			else if (value != null && typeof value == "object")
				this.form2object(value, prefix + field);
			else
				this.component2value(component);
		}

		return obj;
	}

	/**
	 * Binds components to an array.
	 * 
	 * @param array
	 *            value to map as array
	 * @param component
	 *            HTML component
	 */
	OForm.prototype.component2array = function(obj, component, prefix, field) {
		if (component != null && component.is('table')) {
			// RESET THE JSON ARRAY
			var array = obj[field];

			// BROWSE ALL THE COMPONENT
			var componentType = this.fieldTypes[prefix];
			if (componentType != null)
				for (index in array) {
					componentName = prefix + "_" + index;

					if (componentType == 'o')
						array[index] = this.form2object(array[index], componentName);
					else
						array[index] = this.component2value(componentName);

					++index;
				}
		}
	}
	/**
	 * Binds a component to a object field.
	 * 
	 * @param value
	 *            value to map
	 * @param component
	 *            HTML component
	 */
	OForm.prototype.component2value = function(component) {
		if (typeof component == "string") {
			// SEARCH THE COMPONENT
			component = $("#" + component);
			if (component == null) {
				// SEARCH WITH LAST PIECE OF THE NAME
				var lastPiecePos = component.lastIndexOf("_");
				if (lastPiecePos > -1)
					component = $("#" + component.substring(lastPiecePos));
			}
		}

		if (component != null) {
			// SET THE VALUE
			if (component.is('input')) {
				return component.val();
			} else
				// AS TEXT
				return component.text();
		}
	}
}

function formatDocumentContent(doc, editable, fieldColName, valueColName) {
	if (doc == null)
		return "";
	
	if( fieldColName == null )
		fieldColName = "Field";
	if( valueColName == null )
		valueColName = "Value";
	if( editable == null )
		editable = false;

	var out = "<table width='100%'><tr><th>"+fieldColName+"</th><th>"+valueColName+"</th></tr>";

	var fieldValue;
	for (field in doc) {
		fieldValue = doc[field];
		
		out += "<tr><td>";
		out += field;
		out += "</td><td>";

		if( editable ){
			out += "<input id='document_";
			out += field;
			out += "' value='";
		}
		
		if( fieldValue == null)
			out += 'null';		
		else if (fieldValue instanceof Array) {
			for( v in fieldValue ){
				if( v > 0)
					out += ', ';
				
				if( typeof fieldValue[v] == 'object' )
					out+= ('#'+fieldValue[v]['@rid']);
				else
					out += fieldValue[v];
			}
		} else if ( typeof fieldValue == 'object') {
			out += ('#'+fieldValue['@rid']);
		} else
			out += fieldValue;

		if( editable )
			out += "'/>";	

		out += "</td></tr>";
	}

	out += "</table>"
	return out;
}
