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

/**
 * Client-side form binding.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Fabio Ercoli (fabio.ercoli--at--assetdata.it)
 * @author Luca Molino (luca.molino--at--assetdata.it)
 */
function OForm(options) {

	this.object = null;
	this.templateMap = {};
	this.fieldTypes = {};
	this.objectsMetaData = {};

	this.options = {
		debug : false,
		onBeforeAdd : function() {
		},
		onAfterAdd : function() {
		},
		onBeforeRemove : function() {
		},
		onAfterRemove : function() {
		}
	};

	if (options) {
		for (o in options)
			this.options[o] = options[o];
	}

}

OForm.prototype.formtoobject = function(prefix) {
	var values = this.values(prefix);
	var cursor = this.findRoot(this.object, prefix.substring(form
			.getBasePrefix().length));

	if (cursor) {
		this.merge(cursor, values);
	}
}
OForm.prototype.merge = function(cursor, values, parent, key) {
	var form = this;

	if ($.isArray(cursor)) {
		$.each(cursor, function(i, el) {
			if (values[i]) {
				form.merge(cursor[i], values[i], cursor, i);
			} else {
				cursor.splice(i, 1);
			}
		});
		$.each(values, function(i, el) {
			if (!cursor[i]) {
				cursor.push(el);
			}
		});
	} else if (typeof cursor == "object") {
		$.each(cursor, function(i, el) {
			if (values[i])
				form.merge(cursor[i], values[i], cursor, i);
		});
	} else {
		if (parent && key && key != '@type')
			parent[key] = values;
	}

}
OForm.prototype.findRoot = function(currentObject, prefix) {
	if (prefix.length == "")
		return currentObject;
	if (currentObject[prefix]) {
		return currentObject[prefix];
	} else {
		$.each(currentObject, function(i, el) {
			return this.findRoot(el);
		});
		return null;
	}
}
OForm.prototype.values = function(prefix) {
	// the graph taking form values
	var driver = {};

	// forms
	var objects = $("[id^=" + prefix + "]");
	var form = this;

	objects.each(function(i, el) {
		var id = el.id;

		if (id && id.indexOf(prefix + "_") == 0 && id.indexOf("!") < 0
				&& $(el).is(":visible")) {
			var path = id.split('_');
			path.shift();
			var pointer = null;

			$.each(path,
					function(i, stringItem) {
						if (!pointer) {
							pointer = driver;
						}

						if (!pointer[stringItem]) {
							pointer[stringItem] = {};
						}

						if (i == path.length - 1) {
							var value = el.value;
							if (pointer['@class']) {
								var className = pointer['@class'];
								var classInfo = window.form
										.findClassSchema(className);
								if (classInfo != null) {
									var itemType = window.form.findFieldType(
											classInfo, stringItem);
									if (itemType == 'STRING') {
										value = el.value;
									} else if (itemType == 'INTEGER'
											|| itemType == 'SHORT'
											|| itemType == 'LONG'
											|| itemType == 'BYTE') {
										value = parseInt(el.value, 10);
									} else if (itemType == 'FLOAT'
											|| itemType == 'DOUBLE'
											|| itemType == 'LONG') {
										value = parseFloat(el.value);
									} else if (itemType == 'BOOLEAN') {
										value = Boolean(el.value);
									}
								}
							}
							pointer[stringItem] = value;
						} else {
							pointer = pointer[stringItem];
							var verify = path[i + 1];

							if (!parseInt(verify, 10) && verify != 0) {
								pointer['@type'] = 'd';
								if (!pointer['@class']) {
									var classN = form.findClass(id, i + 1);
									if (classN)
										pointer['@class'] = classN;
								}
							}
						}
					});
		}
	});
	return driver;
}
OForm.prototype.getBasePrefix = function() {
	if (this.fieldTypes[""])
		return "";
	var result;
	for (field in this.fieldTypes) {
		result = field;
		break;
	}
	return result;
}
OForm.prototype.getDepth = function(componentName) {
	var counter = 0;
	$.each(componentName.split('_'), function(i, elem) {
		if (parseInt(elem, 10) || elem == '0')
			counter++;
	});
	return counter;
}
OForm.prototype.findClass = function(id, i) {
	var base = this.getBasePrefix();
	if (base && base != "") {
		id = id.substring(base.length);
		i = i - (base.split('_').length - 1)
	}
	return this.findClassInObject(id, i);
}
OForm.prototype.findClassInObject = function(id, i) {
	form = this;
	var path = id.split('_');

	if (form.object) {
		var pointer = form.object;
		for (s = 0; s <= i; s++) {
			var go = 0;
			if (!parseInt(path[s], 10) && path[s] != 0) {
				go = path[s];
			}
			pointer = pointer[go];
		}
		if (pointer['@class'])
			return pointer['@class'];
		else
			return null;
	}
}
OForm.prototype.findClassSchema = function(className) {
	var clazz = null;
	for (i in window.databaseInfo["classes"]) {
		if (window.databaseInfo["classes"][i]["name"] == className) {
			clazz = window.databaseInfo["classes"][i];
			break;
		}
	}
	return clazz;
}
OForm.prototype.findFieldType = function(classInfo, fieldName) {
	var type = null;
	for (i in classInfo["properties"]) {
		if (classInfo["properties"][i]["name"] == fieldName) {
			type = classInfo["properties"][i]["type"];
			break;
		}
	}
	return type;
}
OForm.prototype.arrayAdd = function(componentName) {

	this.options.onBeforeAdd(componentName, index);
	var form = this;

	if (this.templateMap[componentName]) {

		var referenceRow = this.templateMap[componentName];
		var component = $("#" + componentName);
		if (component.size() > 0) {
			var componentChild = component.children().last();
			var html = componentChild.html();
			var toFind = componentName + "_";
			var posixFrom = html.indexOf(toFind) + toFind.length;
			var posixTo = html.indexOf("_", posixFrom);
			if (posixFrom > 0 && posixTo > 0) {
				var value = html.substring(posixFrom, posixTo);
				var indexToFind = "_?" + form.getDepth(componentName);
				var index = parseInt(value, 10) + 1;

				var row = referenceRow;
				while (row.indexOf(indexToFind) > -1)
					row = row.replace(indexToFind, "_" + index);
				row = row.replace(/_\?./g, '_0');

				component.append(row);

				var buttonRemove = document.getElementById(componentName + "_"
						+ index + "!remove");
				if (buttonRemove) {
					$(buttonRemove).bind("click", function(event) {
						event.preventDefault();
						form.arrayRemove($(this));
					});
				}
			}
		}
	}

	this.options.onAfterAdd(componentName, index);
}
OForm.prototype.arrayRemoveElem = function(componentName, index) {
	this.options.onBeforeRemove(componentName, index);

	var component = $("#" + componentName);
	if (component.size() > 0) {
		var target = component.children().eq(index);
		if (target.length > 0) {
			target.hide();
		}
	}

	this.options.onAfterRemove(componentName, index);
}
OForm.prototype.arrayRemove = function(obj) {
	var id = obj.attr("id");

	var from = id.lastIndexOf("_");
	var to = id.indexOf("!", from + 1);

	var componentName = id.substring(0, from);
	var index = id.substring(from + 1, to);

	this.arrayRemoveElem(componentName, parseInt(index, 10));
}
/**
 * Binds an object to the current page. When called recursively, prefix
 * parameter contains the caller object's field.
 */
OForm.prototype.object2form = function(obj, prefix, template, level) {
	if (this.object == null) {
		this.object = obj;
	}

	if (template == null)
		template = "";
	else
		template = template + "_";

	if (prefix == null)
		prefix = "";
	else
		prefix = prefix + "_";

	if (level == null)
		level = 0;

	this.fieldTypes[prefix] = 'o';

	for (field in obj) {
		if (field.charAt(0) == "@") {
			continue;
		}

		var value = obj[field];

		var componentName = prefix + field;
		var component = $("#" + componentName);

		if (value instanceof Array)
			this.array2component(value, component, componentName, level);
		else if (typeof value == "object")
			this.object2form(value, componentName, template + field, level);
		else
			this.value2component(value, component, level);
	}
}

OForm.prototype.bindArray2component = function(array, componentName) {
	this.object = array;
	var component = $("#" + componentName);
	this.array2component(array, component, componentName, 0);
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
	var form = this;

	if (component != null) {
		componentChild = component.children().last();

		var templateRow = this.templateMap[prefix];
		var referenceRow;
		if (templateRow == null) {
			// FIRST TIME: SEARCH THE TEMPLATE
			referenceRow = component.html();
			this.templateMap[prefix] = referenceRow;
		} else
			referenceRow = templateRow;

		if (referenceRow == null) {
			// NOT FOUND
			if (this.options.debug)
				alert("OrientDB Forms: can't find id for template \""
						+ template + "\"");
			return;
		}

		component.empty();

		var indexToFind = "_?" + level;
		for (index in array) {
			var row = referenceRow;
			while (row.indexOf(indexToFind) > -1)
				row = row.replace(indexToFind, "_" + index);

			component.append(row);

			var buttonRemove = document.getElementById(prefix + "_" + index
					+ "!remove");
			if (buttonRemove) {
				$(buttonRemove).bind("click", function(event) {
					event.preventDefault();
					form.arrayRemove($(this));
				});
			}

			var value = array[index];
			if (value != null && typeof value == "object") {
				if (index == 0)
					this.fieldTypes[prefix] = 'o';

				this.object2form(value, prefix + "_" + index, prefix + "_?0",
						level + 1);
			} else {
				if (index == 0)
					this.fieldTypes[prefix] = 'v';
				this.value2component(value, prefix + "_" + index, level + 1);
			}
		}

		if (templateRow == null) {
			var buttonAdd = document.getElementById(prefix + "!add");
			if (buttonAdd) {
				$(buttonAdd).bind("click", function(event) {
					event.preventDefault();
					form.arrayAdd(prefix);
				});
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
		if (component.is('input') || component.is('select')) {
			component.val(value);
		} else
			// AS TEXT
			component.text(value);
	}
}
