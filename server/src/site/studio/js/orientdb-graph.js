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
function OGraph(targetId) {
	this.targetId = targetId;
	this.sigRoot = document.getElementById(targetId);
	this.sigInst = sigma.init(this.sigRoot).drawingProperties({
		defaultLabelColor : '#fff'
	}).graphProperties({
		minNodeSize : 0.5,
		maxNodeSize : 5,
		minEdgeSize : 1,
		maxEdgeSize : 1
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
			var W = 100, H = 100;

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

		OGraph.prototype.hideNodeInfo = function(event) {
			this.popUp && this.popUp.remove();
			this.popUp = false;
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

	OGraph.prototype.drawGraph = function(rootNode, deep) {
		if (!rootNode || !targetId)
			return;

		if (deep)
			// RELOAD THE NODE WITH A FETCH PLAN
			if (typeof rootNode == "string")
				rootNode = orientServer.load(rootNode, "*:" + deep);
			else if (typeof rootNode == "object")
				rootNode = orientServer.load(rootNode["@rid"], "*:" + deep);

		if (rootNode)
			this.drawVertex(rootNode);

		this.sigInst.position(0, 0, 1).activateFishEye().draw();
	}

	OGraph.prototype.drawVertex = function(node, rootNodeId) {
		var nodeId = this.getEndpoint(node);
		if (!nodeId) {
			var nodeId = this.drawEndpoint(node);

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
		var childId = this.drawVertex(child, nodeId);
		if (childId && nodeId) {
			// CREATE THE EDGE
			this.sigInst.addEdge(nodeId + "_" + name + "_" + childId, nodeId,
					childId);
		}
	}

	OGraph.prototype.drawEndpoint = function(node) {
		if (node == null)
			return null;

		var nodeId = null;
		if (node instanceof String && node.charAt(0) == '#')
			nodeId = node;
		else
			nodeId = node["@rid"];

		if (nodeId == null)
			return null;

		this.sigInst.addNode(nodeId, {
			label : nodeId,
			'x' : Math.random(),
			'y' : Math.random(),
			'size' : 0.5 + 4.5 * Math.random(),
			'color' : 'rgb(' + Math.round(Math.random() * 256) + ','
					+ Math.round(Math.random() * 256) + ','
					+ Math.round(Math.random() * 256) + ')'
		});

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

	this.init();
}