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

function OGraph(doc, displayComponent, detailComponent, loadDepthComponent) {
	this.displayComponent = displayComponent;
	this.detailComponent = detailComponent;
	this.doc = null;

	OGraph.prototype.render = function(doc) {
		if (doc == null)
			return;

		this.doc = doc;

		var node = this.document2node(doc);
		// load JSON data
		ht.loadJSON(node);
		ht.compute();
		// emulate a click on the root node.
		ht.onClick(ht.root);
	}

	OGraph.prototype.removeNode = function(nodeid, field) {
		if (this.doc['@rid'] == nodeid) {
			var fieldValue;
			var valueToSearch = '#' + field;
			for (i in this.doc) {
				fieldValue = this.doc[i];

				if (fieldValue instanceof Array) {
					for (e in fieldValue) {
						if (fieldValue[e] == valueToSearch)
							fieldValue.splice(e, 1);
					}
				} else if (fieldValue == valueToSearch)
					this.doc[i] = null;
			}
		}

		ht.op.removeEdge([ [ nodeid, field ] ], {
			type : 'fade:seq',
			duration : 300,
			fps : '30',
			hideLabels : false
		});
	};

	OGraph.prototype.document2node = function(doc) {
		var node = {
			children : [],
			data : {
				document : doc
			}
		};

		var rid;
		var value;
		for (index in doc) {
			value = doc[index];

			if (index == '@rid')
				node['id'] = value;
			else if (index == '@class')
				node['name'] = value + '[' + node['id'] + ']';
			else {
				if (value instanceof Array) {
					var arrayValue;
					for (i in value) {
						arrayValue = value[i];
						if (arrayValue != null) {
							if (typeof arrayValue == 'object')
								node['children'].push(this
										.document2node(arrayValue));
							else if (typeof arrayValue == 'string'
									&& arrayValue.charAt(0) == '#') {
								rid = arrayValue.substring(1);
								node['children'].push({
									id : rid,
									name : '[' + rid + ']',
									data : {
										document : null,
										relation : index
									}
								});
							}
						}
					}
				} else if (value != null && typeof value == 'object') {
					node['children'].push(this.document2node(value));
				} else if (typeof value == 'string' && value.charAt(0) == '#') {
					rid = value.substring(1);

					node['children'].push({
						id : rid,
						name : '[' + rid + ']',
						data : {
							document : null,
							relation : index
						}
					});
				}
			}
		}
		return node;
	}

	var labelType, useGradients, nativeTextSupport, animate;

	(function() {
		var ua = navigator.userAgent, iStuff = ua.match(/iPhone/i)
				|| ua.match(/iPad/i), typeOfCanvas = typeof HTMLCanvasElement, nativeCanvasSupport = (typeOfCanvas == 'object' || typeOfCanvas == 'function'), textSupport = nativeCanvasSupport
				&& (typeof document.createElement('canvas').getContext('2d').fillText == 'function');
		// I'm setting this based on the fact that ExCanvas provides text
		// support for IE
		// and that as of today iPhone/iPad current text support is lame
		labelType = (!nativeCanvasSupport || (textSupport && !iStuff)) ? 'Native'
				: 'HTML';
		nativeTextSupport = labelType == 'Native';
		useGradients = nativeCanvasSupport;
		animate = !(iStuff || !nativeCanvasSupport);
	})();

	var infovis = document.getElementById(displayComponent);
	var w = infovis.offsetWidth - 50, h = infovis.offsetHeight - 50;

	var ht = new $jit.Hypertree(
			{
				// id of the visualization container
				injectInto : displayComponent,
				// canvas width and height
				width : w,
				height : h,
				// Change node and edge styles such as
				// color, width and dimensions.
				Node : {
					dim : 9,
					color : "#f00"
				},
				Edge : {
					lineWidth : 2,
					color : "#088"
				},
				onBeforeCompute : function(node) {
				},
				// Attach event handlers and add text to the
				// labels. This method is only triggered on label
				// creation
				onCreateLabel : function(domElement, node) {
					domElement.innerHTML = node.name;
					$jit.util.addEvent(domElement, 'click', function() {
						ht.onClick(node.id);
					});
				},
				// Change node styles when labels are placed
				// or moved.
				onPlaceLabel : function(domElement, node) {
					var style = domElement.style;
					style.display = '';
					style.cursor = 'pointer';
					if (node._depth <= 1) {
						style.fontSize = "12pt";
						style.color = "#ddd";

					} else if (node._depth == 2) {
						style.fontSize = "10pt";
						style.color = "#555";

					} else {
						style.display = 'none';
					}

					var left = parseInt(style.left);
					var w = domElement.offsetWidth;
					style.left = (left - w / 2) + 'px';
				},

				onAfterCompute : function() {
					// Build the right column relations list.
					// This is done by collecting the information (stored in the
					// data property)
					// for all the nodes adjacent to the centered node.
					var node = ht.graph.getClosestNodeToOrigin("current");
					var html = "<center><h3>" + node.name + "</h3></center>";
					html += "<table width='100%' border='0' cellspacing='5' cellpadding='0' class='ograph_detail'><tr><th>Field</th><th>Relationship</th><th>Actions</th></tr>";
					node
							.eachAdjacency(function(adj) {
								var child = adj.nodeTo;
								if (child.data) {
									var rel = child.data.relation != null ? child.data.relation
											: "(undirected)";
									html += "<tr><td align='center'>"
											+ rel
											+ "</td><td align='center'>"
											+ child.name
											+ "</td><td align='center'><button onClick=\"javascript:graphEditor.removeNode('"
											+ node.id
											+ "', '"
											+ child.id
											+ "')\"><img border='0' alt='Refresh' src='images/remove.png' align='top' /></button></td></tr>";
								}
							});
					html += "</table>";
					$jit.id(detailComponent).innerHTML = html;
				},

				Events : {
					enable : true,
					onClick : function(node, eventInfo, e) {
						if (node == false)
							return;

						var level = 2;
						if (loadDepthComponent != null)
							level = $('#' + loadDepthComponent).val();

						var ans = loadDocument(node.id, level);
						if (ans != null) {
							node.name = ans.name;
						}

						// $jit.id(detailComponent).innerHTML =
						// formatDocumentContent(
						// ans.data.document, true);

						// create json graph to add.
						var trueGraph = ans;
						// perform sum animation.
						ht.op.sum(trueGraph, {
							type : 'fade:con',
							fps : '30',
							duration : 300,
							hideLabels : false,
							onComplete : function() {
							}
						});
					}
				}
			});

	this.render(doc);
}
