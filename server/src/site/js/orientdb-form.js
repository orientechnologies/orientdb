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

function OForm(options) {
	
	this.object = null;
	this.templateMap = {};
	this.fieldTypes = {};
	this.options = {
		debug : false,
		onBeforeAdd : function(){},
		onAfterAdd : function(){},
		onBeforeRemove : function(){},
		onAfterRemove : function(){}
	};

	if (options) {
		for (o in options)
			this.options[o] = options[o];
	}
	
	/*
	$(".arrayRemove").live("click", function(event) {
		event.preventDefault();
		form.arrayRemove($(this));
	});
	*/
	
}

OForm.prototype.arrayAdd = function(componentName) {
		this.options.onBeforeAdd(componentName, index);
	
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
					var indexToFind = "_?" + 0;
					var index = parseInt(value) + 1;
				
					var row = referenceRow;
					while (row.indexOf(indexToFind) > -1)
						row = row.replace(indexToFind, "_" + index);

					component.append(row);
					
					var buttonRemove = document.getElementById(componentName+"_"+index+"!remove");
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
		
	var from = id.indexOf("_");
	var to = id.indexOf("!",  from + 1);
		
	var componentName = id.substring(0, from);
	var index = id.substring(from+1, to);
		
	this.arrayRemoveElem(componentName, parseInt(index));
}
	

/**
 * Binds an object to the current page. When called recursively, prefix
 * parameter contains the caller object's field.
 */
OForm.prototype.object2form = function(obj, prefix, template, level) {
	if (this.object == null)
		this.object = obj;

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
		if (field.charAt(0) == "@")
			continue;

		var value = obj[field];

		var componentName = prefix + field;
		var component = $("#" + componentName);

		if (value instanceof Array)
			this.array2component(value, component, componentName, template
					+ field, level);
		else if (typeof value == "object")
			this.object2form(value, componentName, template + field, level);
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
	OForm.prototype.array2component = function(array, component, prefix,
			template, level) {
		if (component != null) {
			componentChild = component.children().last();

			var referenceRow = this.templateMap[template];
			if (referenceRow == null) {
				// FIRST TIME: SEARCH THE TEMPLATE
				referenceRow = component.html();
				this.templateMap[template] = referenceRow;
			}

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
				
				var buttonRemove = document.getElementById(prefix+"_"+index+"!remove");
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

					this.object2form(value, prefix + "_" + index, template
							+ "_?0", level + 1);
				} else {
					if (index == 0)
						this.fieldTypes[prefix] = 'v';
					this
							.value2component(value, prefix + "_" + index,
									level + 1);
				}
			}
			
			var buttonAdd= document.getElementById(prefix+"!add");
			if (buttonAdd) {
				$(buttonAdd).bind("click", function(event) {
					event.preventDefault();
					form.arrayAdd(prefix);
				});
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
		if (component != null) {
			// RESET THE JSON ARRAY
			var array = obj[field];

			// BROWSE ALL THE COMPONENT
			var componentType = this.fieldTypes[prefix];
			if (componentType != null)
				for (index in array) {
					componentName = prefix + "_" + index;

					if (componentType == 'o')
						array[index] = this.form2object(array[index],
								componentName);
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
