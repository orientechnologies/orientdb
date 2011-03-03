/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
 * Client-side controller to develop rich-client web applications.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */

function OController(options) {
	this.parameters = {};
	this.cachedPages = {};
	this.options = {
		debug : false,
		cachePages : false
	}

	if (options) {
		for (o in options)
			this.options[o] = options[o];
	}

	OController.prototype.loadPage = function(component, file, callback, cache) {
		if (cache == null)
			cache = this.cachePages;

		var content = this.cachedPages[file];

		if (content == null) {
			var me = this;
			$('#' + component).load(file, function(content) {
				if (cache)
					me.cachedPages[file] = content;
				
				try {
					onPageLoad();
				} catch (e) {
				}

				if (callback != null)
					callback();
			});
		} else {
			$('#' + component).html(content);
			$('#' + component).ready(function() {
				if (callback != null)
					callback();
			});
		}

		parent.location.hash = file;
	}

	OController.prototype.parameter = function(name, value) {
		if (value == undefined)
			// GET
			return this.parameters[name];
		else if (value == null)
			// REMOVE
			return this.parameters.slice(name);
		else if (value == null)
			// SET
			return this.parameters[name] = value;
	}
}