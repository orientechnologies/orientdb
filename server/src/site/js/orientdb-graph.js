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

function OGraph(doc, htmlComponent) {
	this.htmlComponent = htmlComponent;

	OGraph.prototype.render = function(doc) {
		if (doc == null)
			return;

		var node = this.document2node(doc);
		// load JSON data
		st.loadJSON(node);
		st.compute();
		// emulate a click on the root node.
		st.onClick(st.root);

		// append information about the root relations in the right column
		$jit.id('inner-details').innerHTML = formatDocumentContent(st.graph
				.getNode(st.root).data.document, true);
	}

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
										document : null
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
							document : null
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

	// Implement a node rendering function called 'nodeline' that plots a
	// straight line
	// when contracting or expanding a subtree.
	$jit.ST.Plot.NodeTypes
			.implement({
				'nodeline' : {
					'render' : function(node, canvas, animating) {
						if (animating === 'expand' || animating === 'contract') {
							var pos = node.pos.getc(true), nconfig = this.node, data = node.data;
							var width = nconfig.width, height = nconfig.height;
							var algnPos = this
									.getAlignedPos(pos, width, height);
							var ctx = canvas.getCtx(), ort = this.config.orientation;
							ctx.beginPath();
							if (ort == 'left' || ort == 'right') {
								ctx.moveTo(algnPos.x, algnPos.y + height / 2);
								ctx.lineTo(algnPos.x + width, algnPos.y
										+ height / 2);
							} else {
								ctx.moveTo(algnPos.x + width / 2, algnPos.y);
								ctx.lineTo(algnPos.x + width / 2, algnPos.y
										+ height);
							}
							ctx.stroke();
						}
					}
				}

			});

	// init Spacetree
	// Create a new ST instance
	var st = new $jit.ST({
		'injectInto' : htmlComponent,
		// set duration for the animation
		duration : 100,
		// set animation transition type
		transition : $jit.Trans.Quart.easeInOut,
		// set distance between node and its children
		levelDistance : 90,
		// set max levels to show. Useful when used with
		// the request method for requesting trees of specific depth
		levelsToShow : 2,
		// set node and edge styles
		// set overridable=true for styling individual
		// nodes or edges
		Node : {
			height : 20,
			width : 40,
			// use a custom
			// node rendering function
			type : 'nodeline',
			color : 'orange',
			lineWidth : 1.5,
			align : "center",
			overridable : true
		},

		Edge : {
			type : 'bezier',
			lineWidth : 1.5,
			color : 'red',
			overridable : true
		},

		// Add a request method for requesting on-demand json trees.
		// This method gets called when a node
		// is clicked and its subtree has a smaller depth
		// than the one specified by the levelsToShow parameter.
		// In that case a subtree is requested and is added to the
		// dataset.
		// This method is asynchronous, so you can make an Ajax request
		// for that
		// subtree and then handle it to the onComplete callback.
		// Here we just use a client-side tree generator (the getTree
		// function).
		request : function(nodeId, level, onComplete) {
			var ans = loadDocument(nodeId, level);
			if (ans != null)
				$jit.id('inner-details').innerHTML = formatDocumentContent(
						ans.data.document, true);
			onComplete.onComplete(nodeId, ans);
		},

		onBeforeCompute : function(node) {
			$jit.id('inner-details').innerHTML = formatDocumentContent(
					node.data.document, true);
		},

		onAfterCompute : function() {
		},

		// This method is called on DOM label creation.
		// Use this method to add event handlers and styles to
		// your node.
		onCreateLabel : function(label, node) {
			label.id = node.id;
			label.innerHTML = node.name;
			label.onclick = function() {
				st.onClick(node.id);
			};
			// set label styles
			var style = label.style;
			style.width = 40 + 'px';
			style.height = 17 + 'px';
			style.cursor = 'pointer';
			style.color = 'black';
			// style.backgroundColor = '#1a1a1a';
			style.fontSize = '10pt';
			style.textAlign = 'center';
			style.textDecoration = 'underline';
			style.paddingTop = '3px';
		},

		// This method is called right before plotting
		// a node. It's useful for changing an individual node
		// style properties before plotting it.
		// The data properties prefixed with a dollar
		// sign will override the global node style properties.
		onBeforePlotNode : function(node) {
			// add some color to the nodes in the path between the
			// root node and the selected node.
			if (node.selected) {
				node.data.$color = "red";
			} else {
				delete node.data.$color;
			}
		},

		// This method is called right before plotting
		// an edge. It's useful for changing an individual edge
		// style properties before plotting it.
		// Edge data proprties prefixed with a dollar sign will
		// override the Edge global style properties.
		onBeforePlotLine : function(adj) {
			if (adj.nodeFrom.selected && adj.nodeTo.selected) {
				adj.data.$color = "green";
				adj.data.$lineWidth = 3;
			} else {
				delete adj.data.$color;
				delete adj.data.$lineWidth;
			}
		}
	});

	// Add event handlers to switch spacetree orientation.
	function get(id) {
		return document.getElementById(id);
	}

	var top = get('r-top'), left = get('r-left'), bottom = get('r-bottom'), right = get('r-right');

	function changeHandler() {
		if (this.checked) {
			top.disabled = bottom.disabled = right.disabled = left.disabled = true;
			st
					.switchPosition(
							this.value,
							"animate",
							{
								onComplete : function() {
									top.disabled = bottom.disabled = right.disabled = left.disabled = false;
								}
							});
		}
	}

	top.onchange = left.onchange = bottom.onchange = right.onchange = changeHandler;

	this.render(doc);
}
