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
function OGraph(targetId, config) {
	this.targetId = targetId;
	this.config = {
		"depth" : 2,
		"bluePrintsGraphModel" : "false",
		"edgeType" : "curve",
		"colors" : {},
		"selectedFields" : {},
		"availableFields" : {},
		"fieldCallback" : 'enableField',
		"fieldsColumns" : 2
	};

	// OVERWRITE CONFIG
	if (config)
		for (c in config)
			this.config[c] = config[c];

	this.sigRoot = document.getElementById(targetId);
	this.sigInst = sigma.init(this.sigRoot).drawingProperties({
		defaultLabelColor : '#fff'
	}).graphProperties({
		minNodeSize : 0.5,
		maxNodeSize : 15,
		minEdgeSize : 1,
		maxEdgeSize : 1,
		defaultEdgeType : this.config.edgeType
	});

	this.popUp = null;

	OGraph.prototype.init = function() {
		// The following method will parse the related sigma instance nodes
		// and set their positions around a circle:
		sigma.publicPrototype.circularLayout = function() {
			var R = 100, i = 0, L = this.getNodesCount();

			this.iterNodes(function(n) {
				n.x = Math.cos(Math.PI * (i++) / L) * R;
				n.y = Math.sin(Math.PI * (i++) / L) * R;
			});

			return this.position(0, 0, 1).draw();
		};

		// The following method will parse the related sigma instance nodes
		// and set its position to as random in a square around the center:
		sigma.publicPrototype.randomLayout = function() {
			var component = $('#' + self.targetId);
			var W = component.width() / 2 - 50, H = component.height() / 2 - 50;

			this.iterNodes(function(n) {
				n.x = W * Math.random();
				n.y = H * Math.random();
			});

			return this.position(0, 0, 1).draw();
		};

		// First, let's write a FishEye class.
		// There is no need to make this class global, since it is made to be
		// used
		// through
		// the SigmaPublic object, that's why a local scope is used for the
		// declaration.
		// The parameter 'sig' represents a Sigma instance.
		var FishEye = function(sig) {
			sigma.classes.Cascade.call(this); // The Cascade class manages the
			// chainable property
			// edit/get function.

			var self = this; // Used to avoid any scope confusion.
			var isActivated = false; // Describes is the FishEye is
			// activated.

			this.p = { // The object containing the properties accessible with
				radius : 300, // the Cascade.config() method.
				power : 3
			};

			function applyFishEye(mouseX, mouseY) { // This method will apply a
				// formula relatively to
				// the mouse position.
				var newDist, newSize, xDist, yDist, dist, radius = self.p.radius, power = self.p.power, powerExp = Math
						.exp(power);

				sig.graph.nodes.forEach(function(node) {
					xDist = node.displayX - mouseX;
					yDist = node.displayY - mouseY;
					dist = Math.sqrt(xDist * xDist + yDist * yDist);

					if (dist < radius) {
						newDist = powerExp / (powerExp - 1) * radius
								* (1 - Math.exp(-dist / radius * power));
						newSize = powerExp / (powerExp - 1) * radius
								* (1 - Math.exp(-dist / radius * power));

						if (!node.isFixed) {
							node.displayX = mouseX + xDist
									* (newDist / dist * 3 / 4 + 1 / 4);
							node.displayY = mouseY + yDist
									* (newDist / dist * 3 / 4 + 1 / 4);
						}

						node.displaySize = Math.min(node.displaySize * newSize
								/ dist, 10 * node.displaySize);
					}
				});
			}
			;

			// The method that will be triggered when Sigma's 'graphscaled' is
			// dispatched.
			function handler() {
				applyFishEye(sig.mousecaptor.mouseX, sig.mousecaptor.mouseY);
			}

			this.handler = handler;

			// A public method to set/get the isActivated parameter.
			this.activated = function(v) {
				if (v == undefined) {
					return isActivated;
				} else {
					isActivated = v;
					return this;
				}
			};

			// this.refresh() is just a helper to draw the graph.
			this.refresh = function() {
				sig.draw(2, 2, 2);
			};
		};

		OGraph.prototype.displayOnlySelected = function(event) {
			var nodes = event.content;
			var neighbors = {};

			this.sigInst.iterEdges(function(e) {
				if (nodes.indexOf(e.source) >= 0
						|| nodes.indexOf(e.target) >= 0) {
					neighbors[e.source] = 1;
					neighbors[e.target] = 1;
				}
			});

			this.sigInst.iterNodes(function(n) {
				if (!neighbors[n.id]) {
					n.hidden = 1;
				} else {
					n.hidden = 0;
				}
			});
		}
		OGraph.prototype.hideNodeInfo = function(event) {
			this.popUp && this.popUp.remove();
			this.popUp = false;
		}

		OGraph.prototype.displayAll = function(event) {
			this.sigInst.iterEdges(function(e) {
				e.hidden = 0;
			});
			this.sigInst.iterNodes(function(n) {
				n.hidden = 0;
			});
		}

		OGraph.prototype.attributesToString = function(obj) {
			var buffer = '<ul>';
			for (a in obj) {
				buffer += '<li>' + a + ' : ' + obj[a] + '</li>';
			}
			return buffer + "</ul>";
		}

		OGraph.prototype.showNodeInfo = function(event) {
			this.popUp && this.popUp.remove();

			var node;
			this.sigInst.iterNodes(function(n) {
				node = n;
			}, [ event.content[0] ]);

			var document = orientServer.load(event.content[0]);
			if (this.config["onLoad"])
				this.config["onLoad"](document);

			this.popUp = $('<div class="node-info-popup"></div>').append(
			// The GEXF parser stores all the attributes in an array named
			// 'attributes'. And since sigma.js does not recognize the key
			// 'attributes' (unlike the keys 'label', 'color', 'size' etc),
			// it stores it in the node 'attr' object :

			this.attributesToString(document)).attr('id',
					'node-info' + this.sigInst.getID()).css({
				'display' : 'inline-block',
				'border-radius' : 3,
				'padding' : 5,
				'background' : '#fff',
				'color' : '#000',
				'box-shadow' : '0 0 4px #666',
				'position' : 'absolute',
				'left' : node.displayX,
				'top' : node.displayY + 15
			});

			$('ul', this.popUp).css('margin', '0 0 0 20px');

			$('#' + targetId).append(this.popUp);
		}

		var self = this;

		this.sigInst.bind('overnodes', function() {
			self.showNodeInfo.apply(self, arguments);
		}).bind('outnodes', function() {
			self.hideNodeInfo.apply(self, arguments);
		});

		// Then, let's add some public method to sigma.js instances :
		sigma.publicPrototype.activateFishEye = function() {
			if (!this.fisheye) {
				var sigmaInstance = this;
				var fe = new FishEye(sigmaInstance._core);
				sigmaInstance.fisheye = fe;
			}

			if (!this.fisheye.activated()) {
				this.fisheye.activated(true);
				this._core.bind('graphscaled', this.fisheye.handler);
				document.getElementById('sigma_mouse_' + this.getID())
						.addEventListener('mousemove', this.fisheye.refresh,
								true);
			}

			return this;
		};

		sigma.publicPrototype.desactivateFishEye = function() {
			if (this.fisheye && this.fisheye.activated()) {
				this.fisheye.activated(false);
				this._core.unbind('graphscaled', this.fisheye.handler);
				document.getElementById('sigma_mouse_' + this.getID())
						.removeEventListener('mousemove', this.fisheye.refresh,
								true);
			}

			return this;
		};

		sigma.publicPrototype.fishEyeProperties = function(a1, a2) {
			var res = this.fisheye.config(a1, a2);
			return res == s ? this.fisheye : res;
		};
	}

	OGraph.prototype.drawGraph = function(rootNode, config) {
		if (!rootNode)
			return;

		if (config)
			for (c in config)
				this.config[c] = config[c];

		this.sigInst.drawingProperties({
			"defaultEdgeType" : this.config.edgeType
		});

		if (this.config.depth)
			// RELOAD THE NODE WITH A FETCH PLAN
			if (typeof rootNode == "string")
				rootNode = orientServer
						.load(rootNode, "*:" + this.config.depth);
			else if (typeof rootNode == "object")
				rootNode = orientServer.load(rootNode["@rid"], "*:"
						+ this.config.depth);

		if (this.config["onLoad"])
			this.config["onLoad"](rootNode);

		if (rootNode) {
			this.drawVertex(rootNode, null, {
				'x' : 0.5,
				'y' : 0.5
			});
		}

		this.sigInst.position(0, 0, 1).draw();
	}

	OGraph.prototype.drawVertex = function(node, rootNodeId, config) {
		var nodeId = this.getEndpoint(node);
		if (!nodeId) {
			var nodeId = this.drawEndpoint(node, config);

			// DRAW CONNECTED NODES
			for (fieldName in node) {
				var fieldValue = node[fieldName];

				if (fieldValue != null) {
					if (fieldValue.constructor == Array) {
						// ARRAY: CHECK FOR SUB-OBJECTS
						for (item in fieldValue) {
							this.drawChildVertex(fieldName + item, node,
									nodeId, fieldValue[item]);
						}
					} else if (typeof fieldValue == "object")
						// CHECK FOR SUB-OBJECT
						this.drawChildVertex(fieldName, node, nodeId,
								fieldValue);

				}
			}
		}

		return nodeId;
	}

	OGraph.prototype.drawChildVertex = function(name, node, nodeId, child) {
		if (this.config.bluePrintsGraphModel == "checked") {
			if (child["out"] && child["out"]["@rid"] == nodeId)
				child = child["in"];
			else if (child["in"] && child["in"]["@rid"] == nodeId)
				child = child["out"];
		}

		var childId = this.drawVertex(child, nodeId);
		if (childId && nodeId) {

			// CREATE THE EDGE
			this.sigInst.addEdge(nodeId + "_" + name + "_" + childId, nodeId,
					childId);
		}
	}

	OGraph.prototype.drawEndpoint = function(node, config) {
		var nodeId = getRID(node);
		if (nodeId == null)
			return null;

		// APPLY ONE COLOR PER CLASS TYPE
		var color = this.config.colors[node["@class"]];
		if (!color) {
			color = 'rgb(' + Math.round(Math.random() * 256) + ','
					+ Math.round(Math.random() * 256) + ','
					+ Math.round(Math.random() * 256) + ')';
			this.config.colors[node["@class"]] = color;
			if (this.config["legendTagId"])
				$('#' + this.config["legendTagId"]).append(
						"<tr><td style='width: 30px; height: 15px; background-color: "
								+ color + ";'></td><td>" + node["@class"]
								+ "</td></tr>");
		}

		var maxColumns = this.config.fieldsColumns;
		var currentColumn = 0;
		var snippet = "";

		for (f in node) {
			var fieldInHtml = f.replace('@', '__');
			var availableField = this.config.availableFields[f];
			var selectedField = this.config.selectedFields[fieldInHtml];

			if (!availableField) {
				this.config.availableFields[f] = true;
				if (this.config.fieldsTagId) {
					if (currentColumn == 0)
						snippet += "<tr>";

					currentColumn++;

					snippet += "<td colspan='"
							+ (2 / maxColumns)
							+ "'></td><td><label class='checkbox'><input id='field"
							+ fieldInHtml + "' type='checkbox'"
							+ (selectedField ? " checked='checked'" : "")
							+ " onClick='" + this.config.fieldCallback + "(\""
							+ fieldInHtml + "\")'>" + f + "</label></td>";

					if (currentColumn >= maxColumns) {
						currentColumn = 0;
						snippet += "<\tr>";
					}

				}
			}
		}

		if (snippet.length > 0)
			$('#' + this.config.fieldsTagId).append(snippet);

		var label = "";
		for (sf in this.config.selectedFields) {
			var fieldValue = this.config.selectedFields[sf];
			var fieldInDocument = sf.replace('__', '@');
			var nodeFieldValue = node[fieldInDocument];
			if (fieldValue && nodeFieldValue) {
				if (label.length > 0)
					label += ", ";
				label += fieldInDocument + ":" + nodeFieldValue;
			}
		}

		if (!config)
			config = {};

		var totalEdges = node["out"] && node["out"].constructor == Array ? node["out"].length
				: 0;
		totalEdges += node["in"] && node["in"].constructor == Array ? node["in"].length
				: 0;

		var size = 0.5 + (totalEdges * 0.1);
		if (size > 15)
			size = 15;

		if (!config)
			config = {};

		var localConfig = {
			'label' : label,
			'x' : Math.random(),
			'y' : Math.random(),
			'size' : size,
			'color' : color
		};

		for (cfg in config)
			localConfig[cfg] = config[cfg];

		this.sigInst.addNode(nodeId, localConfig);

		for (f in node) {
			var field = node[f];
			if (field != null) {
				// DRAW FIELDS
			}
		}

		return nodeId;
	}

	OGraph.prototype.getEndpoint = function(node) {
		if (node == null)
			return null;

		var nodeId = null;
		if (node instanceof String && node.charAt(0) == '#')
			nodeId = node;
		else
			nodeId = node["@rid"];

		if (nodeId == null)
			return null;

		var nodes = this.sigInst.getNodes(nodeId);
		return nodes ? nodeId : null;
	}

	OGraph.prototype.circularLayout = function() {
		this.sigInst.circularLayout();
	}

	OGraph.prototype.randomLayout = function() {
		this.sigInst.randomLayout();
	}

	OGraph.prototype.activateFishEye = function() {
		this.sigInst.activateFishEye();
	}
	OGraph.prototype.deactivateFishEye = function() {
		this.sigInst.desactivateFishEye();
	}
	OGraph.prototype.reset = function() {
		this.config.colors = {};
		this.config.availableFields = {};
		if (this.config["legendTagId"])
			$('#' + this.config["legendTagId"]).text("");
		if (this.config["fieldsTagId"])
			$('#' + this.config["fieldsTagId"]).text("");
		this.sigInst.emptyGraph();
	}

	this.init();
}