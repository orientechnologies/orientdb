// import {OEdge} from './OEdge'
// import {OEdgeMenu} from './OEdgeMenu'
// import {OVertex} from './OVertex'
// import {OVertexMenu} from './OVertexMenu'
// import {OGraphDefaultConfig} from './OGraphDefaultConfig'
//
//
// import * as d3 from 'd3';
//
//
// export class OGraph {
//
//   private viewport;
//   private originElement;
//   private svg;
//   private svgContainer;
//   private config;
//   private metadata;
//   private menuActions;
//   private edgeActions;
//   private topics;
//
//   private vertices;
//   private edges;
//   private links;
//   private nodes;
//   private classesLegends;
//   private force;
//
//   private colors;
//   private selected;
//   private dragNode;
//   private clusterClass;
//   private classesInCanvas;
//   private changer;
//
//   private path;
//   private circle;
//   private menu;
//   private edgeMenu;
//
//   private zoomComponent;
//   private edgesInCanvas;
//   private drag_line;
//   private circleSelected;
//   private classesContainer;
//   private edgesClassContainer;
//   private classesContainerData;
//   private clsLegend;
//
//   private pathG;
//   private edgePath;
//   private parentNode;
//
//
//   constructor(elem, config, metadata, menuActions, edgeActions) {
//     this.viewport = d3.select(elem);
//
//
//     this.originElement = elem;
//     this.svg;
//     this.config = this.merge(config);
//
//
//     this.config.width = $(elem).width();
//     this.metadata = this.getMerger().extend({}, metadata);
//     this.menuActions = menuActions;
//     this.edgeActions = edgeActions;
//     this.topics = {}
//     this.vertices = {};
//     this.edges = {}
//     this.links = [];
//     this.nodes = [];
//     this.classesLegends = [];
//     this.force = d3.layout.force();
//
//
//     this.colors = this.createColors(this.metadata.classes);
//     // this.colors = d3-graph.scale.category20();
//     this.selected = null;
//     this.dragNode = null;
//     this.clusterClass = this.initClusterClass();
//     this.classesInCanvas = {vertices: [], edges: []};
//
//
//     this.changer = this.initChanger();
//   }
//
//   initClusterClass() {
//     var ctoc = {};
//     if (this.metadata) {
//       if (this.metadata.classes) {
//         this.metadata.classes.forEach(function (c) {
//           c.isVertex = this.discoverVertex(c.name);
//           c.clusters.forEach(function (cluster) {
//             ctoc[cluster] = c;
//           })
//         });
//       }
//     }
//
//     return ctoc;
//   }
//
//   createColors(classes) {
//     let val = classes.map((c) => this.hashCode(c.name));
//     val.sort((a, b) => {
//       return a - b;
//     });
//     let color = d3.scale.category20()
//       .domain([val[0], val[val.length - 1]])
//     classes.forEach((c) => {
//       color(this.hash(c.name));
//     })
//     return color;
//   }
//
//   hashCode(str) {
//     var hash = 0;
//     if (str.length == 0) return hash;
//     for (let i = 0; i < str.length; i++) {
//       let char = str.charCodeAt(i);
//       hash = ((hash << 5) - hash) + char;
//       hash = hash & hash; // Convert to 32bit integer
//     }
//     return hash;
//   }
//
//   hash(cls) {
//     return this.hashCode(cls);
//   }
//
//   discoverVertex(clazz) {
//     var sup = clazz;
//     var iterator = clazz;
//     while ((iterator = this.getSuperClazz(iterator)) != "") {
//       sup = iterator;
//     }
//     return sup == 'V';
//   }
//
//   getSuperClazz(clazz) {
//     var metadata = this.metadata;
//
//     var classes = metadata['classes'];
//     var clazzReturn = "";
//     for (var entry in classes) {
//       var name = classes[entry]['name'];
//       if (clazz == name) {
//         clazzReturn = classes[entry].superClass;
//         break;
//       }
//     }
//
//     return clazzReturn;
//   }
//
//   initChanger() {
//     var change = [];
//     change['display'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//       this.config.classes[clazz].display = val;
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('.vlabel-outside')
//         .attr('class', 'vlabel-outside')
//         .text(this.bindRealName);
//
//       d3.selectAll('text.elabel-' + clazz.toLowerCase())
//         .selectAll('textPath')
//         .text(function (e) {
//           return this.bindRealNameOrClazz(e.edge);
//         });
//
//     }
//
//     change['displayExpression'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//       this.config.classes[clazz].displayExpression = val;
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('.vlabel-outside')
//         .attr('class', 'vlabel-outside')
//         .text(this.bindRealName);
//
//       d3.selectAll('text.elabel-' + clazz.toLowerCase())
//         .selectAll('textPath')
//         .text(function (e) {
//           return this.bindRealNameOrClazz(e.edge);
//         });
//
//     }
//     change['displayColor'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//       this.config.classes[clazz].displayColor = val;
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('.vlabel-outside')
//         .style("fill", function (d) {
//           return this.bindColor(d, "displayColor");
//         })
//
//     }
//     change['displayBackground'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//       this.config.classes[clazz].displayBackground = val;
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('rect.vlabel-outside-bbox')
//         .style("fill", this.bindRectColor)
//         .style("stroke", this.bindRectColor)
//     }
//
//     change['icon'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//       this.config.classes[clazz].icon = val;
//
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('.vlabel-icon')
//         .attr('class', 'vlabel-icon vicon')
//         .text(val)
//
//
//     }
//     change['iconSize'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//
//       this.config.classes[clazz].iconSize = val;
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('.vlabel-icon')
//         .style("font-size", this.config.classes[clazz].iconSize || 30);
//
//     }
//
//     change['iconCss'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//
//       this.config.classes[clazz].iconCss = val;
//
//     }
//     change['iconVPadding'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//
//       this.config.classes[clazz].iconVPadding = val;
//
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('.vlabel-icon')
//         .attr('y', function (d) {
//           var iconPadding = this.getClazzConfigVal(this.getClazzName(d), "iconVPadding");
//           return iconPadding || 10;
//         })
//
//     }
//     change['strokeWidth'] = function (clazz, prop, val) {
//
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//
//       this.config.classes[clazz].strokeWidth = val;
//
//       d3.selectAll('path.edge-' + clazz.toLowerCase())
//         .style('stroke-width', function (d) {
//           return this.bindStrokeWidth(d.edge);
//         })
//     }
//     var style = function (clazz, prop, val) {
//
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//       this.config.classes[clazz][prop] = val;
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('.vcircle')
//         .style(prop, val);
//
//       d3.selectAll('g.legend-' + clazz.toLowerCase())
//         .selectAll("circle")
//         .style(prop, val);
//
//       d3.selectAll('g.legend-' + clazz.toLowerCase())
//         .selectAll("line")
//         .style(prop, val);
//
//       d3.selectAll('path.edge-' + clazz.toLowerCase())
//         .style(prop, function (d) {
//           return this.bindStroke(d.edge);
//         })
//     }
//     change['fill'] = style;
//     change['stroke'] = style;
//     change['r'] = function (clazz, prop, val) {
//       if (!this.config.classes[clazz]) {
//         this.config.classes[clazz] = {}
//       }
//       this.config.classes[clazz][prop] = val;
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('.vcircle')
//         .attr('r', this.bindRadius);
//
//       this.setSelected(this.selected);
//
//       d3.selectAll('g.vertex-' + clazz.toLowerCase())
//         .selectAll('g.vlabel-outside-group')
//         .attr('y', function (d) {
//           return parseInt(this.bindRadius(d)) + 15;
//         })
//
//
//     }
//     return change;
//   }
//
//   merge(config) {
//     return config ? this.getMerger().extend({}, OGraphDefaultConfig(), config) : OGraphDefaultConfig();
//   }
//
//   getMerger() {
//     return angular ? angular : $;
//   }
//
//   toggleLegend() {
//
//
//     this.svgContainer.selectAll("g.legend-container").attr("class", function () {
//       var cls = d3.select(this).attr("class");
//       return cls.indexOf("hide") != -1 ? "legend-container" : "legend-container hide";
//     })
//     //var parent = d3-graph.select(this.classesContainer.node().parentNode());
//     //
//     //console.log(parent);
//     //var cls = parent.attr("class");
//     //if (cls.indexOf("hide") != -1) {
//     //  parent.attr("class", "legend-container");
//     //} else {
//     //  parent.attr("class", "legend-container hide");
//     //}
//
//   }
//   fullScreen(full) {
//     if (full) {
//       var start = $(this.originElement).offset().top;
//       var wHeight = $(document).height();
//       var height = wHeight - start;
//       this.svgContainer
//         .attr('height', height)
//     } else {
//       this.svgContainer
//         .attr('height', this.config.height)
//     }
//
//   }
//
//   releasePhysicsInternal() {
//     this.svgContainer.selectAll("g.vertex").classed("fixed", function (v) {
//       return v.fixed = false
//     })
//   }
//
//   freezePhysicsInternal() {
//     this.svgContainer.selectAll("g.vertex").classed("fixed", function (v) {
//       return v.fixed = true
//     })
//     this.force.stop();
//   }
//
//   resetZoomInternal() {
//     var b = this.graphBounds();
//     var w = b.X - b.x, h = b.Y - b.y;
//
//
//     var bbox = this.svgContainer.node().getBoundingClientRect();
//     var cw = bbox.width, ch = bbox.height;
//     var s = Math.min(cw / w, ch / h);
//     var tx = (-b.x * s + (cw / s - w) * s / 2), ty = (-b.y * s + (ch / s - h) * s / 2);
//
//     this.svgContainer.transition()
//       .duration(750)
//       .call(this.zoomComponent.translate([tx, ty]).scale(s).event);
//
//   }
//   graphBounds() {
//     var x = Number.POSITIVE_INFINITY, X = Number.NEGATIVE_INFINITY, y = Number.POSITIVE_INFINITY, Y = Number.NEGATIVE_INFINITY;
//
//     d3.selectAll("g.vertex").each(function (v) {
//       x = Math.min(x, v.x - 100);
//       X = Math.max(X, v.x + 100);
//       y = Math.min(y, v.y - 300);
//       Y = Math.max(Y, v.y + 300);
//     });
//     return {x: x, X: X, y: y, Y: Y};
//   }
//
//   addVertex = function (v) {
//     if (!this.get[v["@rid"]]) {
//       this.nodes.push(v);
//       this.put([v["@rid"]], v);
//     }
//   }
//   addEdge(e) {
//     var v1 = e.right ? e.source : e.target;
//     var v2 = e.right ? e.target : e.source;
//
//     var id = v1["@rid"] + "_" + v2["@rid"];
//     var count = this.edges[id];
//     if (!count) {
//       this.edges[id] = [];
//     }
//     var found = false;
//     var l = e.label.replace("in_", "").replace("out_", "");
//
//     if (l == "") l = "E";
//     this.edges[id].forEach(function (e1) {
//       var l1 = e1.label.replace("in_", "").replace("out_", "");
//       if (l1 == "") l1 = "E";
//       if (e1.source == e.source && e1.target == e.target && l == l1 && e.edge["@rid"] === e1.edge["@rid"]) {
//         found = true;
//       }
//     })
//     if (!found) {
//       this.edges[id].push(e);
//       this.links.push(e);
//     }
//   }
//
//   setSelected(v) {
//     var newSel = v != this.selected;
//     this.selected = v;
//     this.refreshSelected(newSel);
//     this.edgeMenu.hide();
//   }
//   get(k) {
//     return this.vertices[k];
//   }
//   put(k, v) {
//     this.vertices[k] = v;
//   }
//   delete(k) {
//     delete this.vertices[k];
//   }
//   clearGraph() {
//     this.clearSelection();
//     this.vertices = {};
//     this.edges = {};
//     this.classesInCanvas = {vertices: [], edges: []}
//     this.edgesInCanvas = [];
//     this.nodes.splice(0, this.nodes.length)
//     this.links.splice(0, this.links.length)
//   }
//   simulate(forceTick) {
//
//     var mst = 100
//     var mas = 60
//     var mtct = 1000 / mas
//     var now = function () {
//       return Date.now();
//     }
//
//     var tick = this.force.tick;
//
//     this.force.tick = function () {
//
//
//       var startTick = now()
//       var step = mst
//       while (step-- && (now() - startTick < mtct)) {
//         if (tick()) {
//           mst = 2
//           return true
//         }
//       }
//       var rnd = Math.floor((Math.random() * 100) + 1);
//       if (rnd % 2 == 0) {
//         this.tick();
//       }
//
//       if (forceTick == true) {
//         this.tick();
//       }
//       return false;
//     }
//   }
//
//   init() {
//
//     this.force.nodes(this.nodes)
//       .links(this.links)
//       .size([this.config.width, this.config.height])
//       .linkDistance(this.config.linkDistance)
//       .linkStrength(0.1)
//       .charge(this.config.charge)
//       .friction(this.config.friction)
//
//
//     this.svgContainer = this.viewport.append('svg');
//
//     // define arrow markers for graph links
//     this.svgContainer.append('svg:defs').append('svg:marker')
//       .attr('id', 'end-arrow')
//       .attr('viewBox', '0 -5 10 10')
//       .attr('refX', 6)
//       .attr('markerWidth', 3)
//       .attr('markerHeight', 3)
//       .attr('orient', 'auto')
//       .append('svg:path')
//       .attr('d', 'M0,-5L10,0L0,5')
//       .attr('fill', '#000')
//       .attr('class', 'end-arrow');
//
//     this.svgContainer.append('svg:defs').append('svg:marker')
//       .attr('id', 'start-arrow')
//       .attr('viewBox', '0 -5 10 10')
//       .attr('refX', 4)
//       .attr('markerWidth', 3)
//       .attr('markerHeight', 3)
//       .attr('orient', 'auto')
//       .append('svg:path')
//       .attr('d', 'M10,-5L0,0L10,5')
//       .attr('class', 'end-arrow');
//
//     // define arrow markers for graph links
//     this.svgContainer.append('svg:defs').append('svg:marker')
//       .attr('id', 'end-arrow-hover')
//       .attr('viewBox', '0 -5 10 10')
//       .attr('refX', 6)
//       .attr('markerWidth', 3)
//       .attr('markerHeight', 3)
//       .attr('orient', 'auto')
//       .append('svg:path')
//       .attr('d', 'M0,-5L10,0L0,5')
//       .attr('class', 'end-arrow-hover');
//
//     this.svgContainer.append('svg:defs').append('svg:marker')
//       .attr('id', 'start-arrow-hover')
//       .attr('viewBox', '0 -5 10 10')
//       .attr('refX', 4)
//       .attr('markerWidth', 3)
//       .attr('markerHeight', 3)
//       .attr('orient', 'auto')
//       .append('svg:path')
//       .attr('d', 'M10,-5L0,0L10,5')
//       .attr('fill', '#000')
//       .attr('class', 'end-arrow-hover');
//
//
//     this.svg = this.svgContainer
//       .attr('width', "100%")
//       .attr('height', "100%")
//       .append("g");
//
//     // line displayed when dragging new nodes
//     this.drag_line = this.svg.append('svg:path')
//       .attr('class', 'link dragline hidden')
//       .attr('d', 'M0,0L0,0');
//
//
//     this.svgContainer.on("click", function () {
//       this.clearSelection();
//       this.clearArrow();
//     })
//     this.svgContainer.on('mousemove', function () {
//
//       if (!this.dragNode) return;
//       // update drag line
//       this.drag_line.attr('d', 'M' + this.dragNode.x + ',' + this.dragNode.y + 'L' + d3.mouse(this.svg.node())[0] + ',' + d3.mouse(this.svg.node())[1]);
//
//
//     });
//     this.circleSelected = this.svg.append('svg:g').append("svg:circle")
//       .attr("class", "selected-vertex selected-vertex-none")
//       .attr('r', this.getConfigVal("node").r + 3);
//
//     this.classesContainer = this.svgContainer.append('svg:g')
//       .attr("class", "legend-container")
//       .attr("transform", function () {
//         return "translate(" + (30) + ",30)";
//       }).selectAll('g');
//
//
//     this.edgesClassContainer = this.svgContainer.append('svg:g')
//       .attr("class", "legend-edge-container")
//       .attr("transform", function () {
//         return "translate(" + (this.config.width - 120) + ",30)";
//       }.bind(this)).selectAll('g');
//
//     this.path = this.svg.append('svg:g').selectAll('g');
//     this.circle = this.svg.append('svg:g').selectAll('g');
//
//     if (this.menuActions) {
//       this.menu = new OVertexMenu(this);
//     }
//     if (this.edgeActions) {
//       this.edgeMenu = new OEdgeMenu(this);
//     }
//
//   }
//
//   clearArrow() {
//
//     if (this.dragNode) {
//
//       this.dragNode = null;
//       this.drag_line
//         .classed('hidden', true)
//         .style('marker-end', '');
//     }
//   }
//
//   getClazzName(d) {
//     if (d['@class']) {
//       return d['@class'];
//     }
//     if (d.source['@class']) {
//       return d.source['@class'];
//     }
//     else {
//       var cluster = d["@rid"].replace("#", "").split(":")[0];
//       var cfg = this.clusterClass[cluster];
//       return cfg ? cfg.name : null;
//     }
//   }
//
//   bindClassName(d) {
//
//     d.elem = this;
//     var cname = this.getClazzName(d);
//     var css = this.getClazzConfigVal(cname, "css", null);
//     var cls = 'vertex ';
//     if (cname) {
//       cls += 'vertex-' + cname.toLowerCase();
//     }
//     return css ? cls + ' ' + css : cls;
//   }
//
//
//   countRel(d) {
//     var v1 = d.right ? d.source : d.target;
//     var v2 = d.right ? d.target : d.source;
//
//     var id = v1["@rid"] + "_" + v2["@rid"];
//     var len = this.edges[id].length
//     return len;
//   }
//
//   countRelInOut(d) {
//     var v1 = d.right ? d.source : d.target;
//     var v2 = d.right ? d.target : d.source;
//
//     var id = v1["@rid"] + "_" + v2["@rid"];
//     var id1 = v2["@rid"] + "_" + v1["@rid"];
//     var len = (this.edges[id] ? this.edges[id].length : 0 ) + (this.edges[id1] ? this.edges[id1].length : 0)
//     return len;
//   }
//
//
//   calculateRelPos(d) {
//     var v1 = d.right ? d.source : d.target;
//     var v2 = d.right ? d.target : d.source;
//
//     var id = v1["@rid"] + "_" + v2["@rid"];
//
//     return this.edges[id].indexOf(d);
//   }
//
//   bindLabel(d) {
//
//     // TO REMOVEe ?
//     d.elem = this;
//     var len = this.countRel(d);
//     var replaced = d.label.replace("in_", "").replace("out_", "")
//     return (replaced != "" ? replaced : "E");//+ ( len > 1 ? " (+" + (len - 1) + ")" : "");
//
//   }
//
//   clearSelection() {
//     this.selected = null;
//     this.menu.hide();
//     this.edgeMenu.hide();
//   }
//   startEdgeCreation() {
//
//
//     this.dragNode = this.selected;
//     this.clearSelection();
//     // reposition drag line
//     this.drag_line
//       .style('marker-end', 'url(#end-arrow)')
//       .classed('hidden', false)
//       .attr('d', 'M' + this.dragNode.x + ',' + this.dragNode.y + 'L' + this.dragNode.x + ',' + this.dragNode.y);
//
//   }
//   endEdgeCreation() {
//     this.clearSelection();
//     this.clearArrow();
//     this.drag_line
//       .classed('hidden', true)
//       .style('marker-end', '');
//   }
//
//   isConnected(node1, node2) {
//
//     if (node1["@rid"] === node2["@rid"])return true;
//     var k1 = node1["@rid"] + "_" + node2["@rid"];
//     var k2 = node2["@rid"] + "_" + node1["@rid"];
//     return this.edges[k1] || this.edges[k2];
//   }
//
//   isInOrOut(node, edge) {
//
//     return node["@rid"] === edge.source["@rid"] || node["@rid"] === edge.target["@rid"];
//   }
//
//   drawInternal() {
//
//     this.path = this.path.data(this.links);
//     this.circle = this.circle.data(this.nodes, function (d) {
//       return d['@rid'];
//     });
//
//
//     this.classesLegends.splice(0, this.classesLegends.length);
//     this.classesLegends = this.classesInCanvas.vertices.concat(this.classesInCanvas.edges);
//
//
//     if (this.classesContainerData) {
//       this.classesContainerData.remove()
//     }
//     this.classesContainerData = this.classesContainer.data(this.classesLegends);
//
//
//     this.clsLegend = this.classesContainerData.enter().append("svg:g").attr("class", function (d) {
//       return "legend legend-" + d.toLowerCase();
//     })
//
//
//     // Vertex Class
//     this.clsLegend.attr("transform", function (d, i) {
//       return "translate(0," + 25 * i + ")";
//     })
//
//
//     this.clsLegend.append("circle")
//       .attr("r", 10)
//       .attr('y', function (d, i) {
//
//       })
//       .attr("class", function (d) {
//         return this.classesInCanvas.vertices.indexOf(d) == -1 ? "elem-invisible" : "";
//       })
//       .style("fill", function (d) {
//         var fill = this.getClazzConfigVal(d, "fill");
//         return fill ? fill : null;
//       })
//       .style("stroke", function (d) {
//         var stroke = this.getClazzConfigVal(d, "stroke");
//         return stroke ? stroke : null;
//       })
//
//     this.clsLegend.append("line")
//       .attr("x1", -10)
//       .attr("x2", 10)
//       .attr("y1", 0)
//       .attr("y2", 0)
//       .attr("class", function (d) {
//         return this.classesInCanvas.edges.indexOf(d) == -1 ? "elem-invisible" : "";
//       })
//       .style("stroke-width", 5)
//       .style("fill", function (d) {
//         var fill = this.getClazzConfigVal(d, "fill");
//         return fill ? fill : null;
//       })
//       .style("stroke", function (d) {
//         var stroke = this.getClazzConfigVal(d, "stroke");
//         return stroke ? stroke : null;
//       })
//
//     var txt = this.clsLegend.append("text")
//       .attr("dy", 5)
//       .text(function (d) {
//         return d;
//       })
//     txt.each(function () {
//       var diff = 15;
//       d3.select(this).attr("dx", diff);
//
//     });
//
//
//     this.pathG = this.path.enter().append('svg:g').attr("class", function (d) {
//       return 'edge-path';
//     });
//
//     this.edgePath = this.pathG.append('svg:path')
//       .attr("class", function (d) {
//         var eclass = d.edge ? "edge" : "edge lightweight"
//         return eclass + " edge-" + d.label.toLowerCase();
//       })
//       .attr("id", function (d, i) {
//         return "linkId_" + i;
//       })
//       .style('marker-start', function (d) {
//         return d.left ? 'url(#start-arrow)' : '';
//       })
//       .style('marker-end', function (d) {
//         return d.right ? 'url(#end-arrow)' : '';
//       })
//
//       .style('stroke', function (d) {
//         return this.bindStroke(d.edge);
//       })
//       .style("stroke-width", function (d) {
//         return this.bindStrokeWidth(d.edge);
//       })
//
//
//     this.pathG.append('svg:path')
//       .attr("class", function (d) {
//         return "path-overlay pointer";
//       })
//       .style('fill', "none")
//       .style('stroke', function (d) {
//         return this.bindStroke(d.edge);
//       })
//       .style("stroke-width", function (d) {
//         return parseInt(this.bindStrokeWidth(d.edge)) + 15;
//       })
//       .on("mouseover", function (d) {
//
//         d3.select(this).style("opacity", "0.3");
//         //var eclass = d.edge ? "edge" : "edge lightweight"
//         //eclass = eclass + " edge-hover edge-" + d.label.toLowerCase();
//         //d3-graph.select(this).attr("class", eclass)
//         //  .style('marker-start', function (d) {
//         //    return d.left ? 'url(#start-arrow-hover)' : '';
//         //  }).style('marker-end', function (d) {
//         //  return d.right ? 'url(#end-arrow-hover)' : '';
//         //});
//       })
//
//
//       .on("mouseout", function (d) {
//
//         d3.select(this).style("opacity", "0");
//         //var eclass = d.edge ? "edge" : "edge lightweight"
//         //eclass += " edge-" + d.label.toLowerCase();
//         //d3-graph.select(this).attr("class", eclass)
//         //  .style('marker-start', function (d) {
//         //    return d.left ? 'url(#start-arrow)' : '';
//         //  }).style('marker-end', function (d) {
//         //  return d.right ? 'url(#end-arrow)' : '';
//         //});
//       })
//       .on("click", function (e) {
//         d3.event.stopPropagation();
//
//         var node = d3.select(this.parentNode).select("text.elabel").node();
//         this.edgeMenu.select({elem: node, d: e})
//         if (this.topics['edge/click']) {
//           this.topics['edge/click'](e);
//         }
//       });
//
//
//     this.pathG.append('svg:text')
//       .attr("class", function (d) {
//         var cls = this.getClazzName(d.edge);
//         var clsEdge = cls ? cls.toLowerCase() : "-e";
//         return "elabel elabel-" + clsEdge;
//       })
//
//       .style("text-anchor", "middle")
//       .attr("dy", "-8")
//       .append("textPath")
//       .attr("startOffset", "50%")
//       .attr("xlink:href", function (d, i) {
//         return "#linkId_" + i;
//       })
//       .text(function (e) {
//         return this.bindRealNameOrClazz(e.edge);
//       }).on("click", function (e) {
//       d3.event.stopPropagation();
//       this.edgeMenu.select({elem: this, d: e})
//       if (this.topics['edge/click']) {
//         this.topics['edge/click'](e);
//       }
//     });
//
//     this.path.exit().remove();
//
//
//     var g = this.circle.enter().append('svg:g').attr('class', this.bindClassName);
//
//
//     g.on('mouseover', function (v) {
//
//
//       if (this.dragNode) {
//         var r = this.bindRadius(v);
//         r = parseInt(r);
//         var newR = r + ((r * 20) / 100);
//         d3.select(v.elem).selectAll('circle').attr('r', newR)
//       }
//
//
//       d3.selectAll("g.vertex").style("opacity", function (n) {
//         return this.isConnected(v, n) ? 1 : 0.1;
//       })
//
//       d3.selectAll("g.menu").style("opacity", function (n) {
//         return 0.1;
//       })
//       d3.selectAll("path.edge").style("opacity", function (edge) {
//         return this.isInOrOut(v, edge) ? 1 : 0.1;
//       });
//       d3.selectAll("text.elabel").style("opacity", function (edge) {
//         return this.isInOrOut(v, edge) ? 1 : 0.1;
//       });
//     });
//
//
//     g.on('mouseout', function (v) {
//       if (this.dragNode) {
//         d3.select(v.elem).selectAll('circle').attr('r', this.bindRadius);
//       }
//
//
//       if (!d3.select(this).classed("dragging")) {
//         d3.selectAll("g.vertex").style("opacity", function (n) {
//           return 1;
//         })
//         d3.selectAll("g.menu").style("opacity", function (n) {
//           return 1;
//         })
//         d3.selectAll("path.edge").style("opacity", function (n) {
//           return 1;
//         });
//         d3.selectAll("text.elabel").style("opacity", function (n) {
//           return 1;
//         });
//       }
//     })
//
//     var drag = this.force.drag();
//
//     drag.on("dragstart", function (v) {
//       d3.event.sourceEvent.stopPropagation();
//       d3.select(this).classed("dragging", true);
//       d3.select(this).classed("fixed", v.fixed = true);
//     })
//     drag.on("dragend", function (v) {
//       d3.event.sourceEvent.stopPropagation();
//       d3.select(this).classed("dragging", false);
//       //d3-graph.select(this).classed("fixed", v.fixed = false);
//     })
//     g.call(drag);
//     var cc = this.clickcancel();
//
//     g.on('dblclick', function () {
//       d3.event.stopPropagation();
//     });
//     g.call(cc);
//
//     g.on('click', function (v) {
//
//       if (this.dragNode && this.topics['edge/create']) {
//         this.topics['edge/create'](this.dragNode, v);
//         d3.event.stopPropagation();
//         this.dragNode = null;
//         d3.select(v.elem).selectAll('circle').attr('r', this.bindRadius);
//       }
//     })
//     cc.on('click', function (e, v) {
//
//       if (this.topics['node/click']) {
//         if (v.loaded) {
//           this.topics['node/click'](v);
//         } else {
//           if (this.topics['node/load']) {
//             this.topics['node/load'](v, function (res) {
//               if (this.isVertex(res)) {
//                 v.loaded = true;
//                 v.source = res;
//                 d3.select(v.elem).attr('class', this.bindClassName);
//                 d3.select(v.elem).selectAll('circle')
//                   .attr('stroke-dasharray', this.bindDashArray)
//                   .attr('fill-opacity', this.bindOpacity)
//                   .attr('r', this.bindRadius);
//                 this.topics['node/click'](v);
//               }
//             });
//           }
//         }
//         this.setSelected(v);
//       }
//
//     });
//     cc.on('dblclick', function (e, v) {
//
//       if (this.topics['node/dblclick']) {
//
//         this.topics['node/dblclick'](v);
//
//       }
//     });
//     g.append('svg:circle')
//       .attr('class', "vcircle")
//       .attr('r', this.bindRadius)
//       .attr('stroke-dasharray', this.bindDashArray)
//       .attr('fill-opacity', this.bindOpacity)
//       .style('fill', this.bindFill)
//       .style('stroke', this.bindStroke);
//
//
//     g.append('svg:text')
//       .attr('x', 0)
//       .attr('y', function (d) {
//         var iconPadding = this.getClazzConfigVal(this.getClazzName(d), "iconVPadding");
//         return iconPadding || 10;
//       })
//       .attr('class', function (d) {
//         var name = this.getClazzConfigVal(this.getClazzName(d), "icon");
//         return 'vlabel-icon vicon' + (!name ? ' elem-invisible' : '')
//         bind;
//       })
//       .style("font-size", function (d) {
//         var size = this.getClazzConfigVal(this.getClazzName(d), "iconSize");
//         return size || 30;
//       })
//       .text(this.bindIcon);
//
//
//     var group = g.append("g");
//     group.attr('class', "vlabel-outside-group");
//
//     function bindBboxPos(prop) {
//       return function (elem) {
//         var node = d3.select(this.parentNode)
//           .selectAll('text.vlabel-outside')
//           .node();
//         var bbox = node.getBBox();
//         return bbox[prop];
//       }
//     }
//
//
//     group.append('svg:text')
//       .attr('x', 0)
//       .attr('y', function (d) {
//         return parseInt(this.bindRadius(d)) + 15;
//       })
//       .attr('class', function (d) {
//         return 'vlabel-outside';
//       })
//       .style("fill", function (d) {
//         return this.bindColor(d, "displayColor");
//       })
//       .text(this.bindRealName);
//
//     group.append('rect')
//       .attr('x', bindBboxPos("x"))
//       .attr('y', bindBboxPos("y"))
//
//       .attr('class', "vlabel-outside-bbox")
//       .attr('height', bindBboxPos("height"))
//       .attr('width', bindBboxPos("width"))
//       .style("fill-opacity", 0.5)
//       .style("stroke-width", 1)
//       .style("stroke", this.bindRectColor)
//       .style("fill", this.bindRectColor)
//
//     this.circle.exit().remove();
//     this.zoomComponent = d3.behavior.zoom()
//       .scaleExtent([0.005, 500])
//       .on("zoom", this.zoom);
//
//     this.svgContainer.call(this.zoomComponent);
//   }
//
//   clickcancel() {
//     var event = d3.dispatch('click', 'dblclick');
//
//     function cc(selection) {
//       var down,
//         tolerance = 5,
//         last,
//         wait = null;
//       // euclidean distance
//       function dist(a, b) {
//         return Math.sqrt(Math.pow(a[0] - b[0], 2), Math.pow(a[1] - b[1], 2));
//       }
//
//
//       selection.on('mousedown', function () {
//         down = d3.mouse(document.body);
//         last = +new Date();
//       });
//       selection.on('mouseup', function (v) {
//         if (dist(down, d3.mouse(document.body)) > tolerance) {
//           return;
//         } else {
//
//           if (wait) {
//             window.clearTimeout(wait);
//             wait = null;
//             event.dblclick(d3.event, v);
//           } else {
//             wait = window.setTimeout((function (e) {
//               return function () {
//                 event.click(e, v);
//                 wait = null;
//               };
//             })(d3.event), 300);
//           }
//         }
//       });
//     };
//     return d3.rebind(cc, event, 'on');
//   }
//
//   bindRadius(d) {
//
//     var radius = this.getClazzConfigVal(this.getClazzName(d), "r", null);
//     return radius ? radius : this.getConfigVal("node").r;
//   }
//
//   bindOpacity(d) {
//     if (!d.loaded) return '0.5';
//
//     return "1";
//   }
//
//   bindFill(d) {
//     var clsName = this.getClazzName(d);
//     var fill = this.getClazzConfigVal(clsName, "fill", null);
//     if (!fill) {
//       fill = d3.rgb(this.colors(this.hash(clsName))).toString();
//       this.changeClazzConfig(clsName, "fill", fill);
//     }
//     return fill;
//   }
//
//   bindStroke(d) {
//     var clsName = this.getClazzName(d);
//     var stroke = this.getClazzConfigVal(clsName, "stroke", null);
//     if (!stroke) {
//
//       stroke = d3.rgb(this.colors(this.hash(clsName))).darker().toString();
//
//       this.changeClazzConfig(clsName, "stroke", stroke);
//     }
//     return stroke;
//
//   }
//
//   bindColor(d, prop, def) {
//
//     var clsName = this.getClazzName(d);
//     var stroke = this.getClazzConfigVal(clsName, prop, null);
//     if (!stroke) {
//       var ret = def || "rgb(0, 0, 0)";
//       return ret;
//     }
//     return stroke;
//
//   }
//
//   bindStrokeWidth(d) {
//     var clsName = this.getClazzName(d);
//     var stroke = this.getClazzConfigVal(clsName, "strokeWidth", null);
//     return stroke || 3;
//
//   }
//
//   bindDashArray(d) {
//     if (!d.loaded) return '5,5';
//     return "0";
//   }
//
//
//   bindRealName(d) {
//
//
//     var name = this.getClazzConfigVal(this.getClazzName(d), "displayExpression", null);
//
//
//     if (name && name !== "") {
//       name = S(name).template(d.source);
//     } else {
//       name = this.getClazzConfigVal(this.getClazzName(d), "display", d.source);
//     }
//     var rid;
//     if (d['@rid'].startsWith("#-")) {
//       var props = Object.keys(d.source).filter(function (e) {
//         return !e.startsWith("@");
//       })
//       rid = (props.length > 0 && d.source[props[0]]) ? d.source[props[0]] : d['@rid'];
//     } else {
//       rid = d['@rid'];
//     }
//
//     return name != null ? name : rid;
//   }
//
//   bindRealNameOrClazz(d) {
//
//     var clazz = this.getClazzName(d);
//
//     var name = this.getClazzConfigVal(this.getClazzName(d), "displayExpression", null);
//
//     if (name && name !== "") {
//       name = S(name).template(d);
//     } else {
//       name = this.getClazzConfigVal(clazz, "display", d);
//     }
//     return name != null ? name : clazz;
//   }
//
//   bindIcon(d) {
//
//     var name = this.getClazzConfigVal(this.getClazzName(d), "icon", null);
//     return name;
//   }
//
//
//   calculateEdgePath(padding) {
//
//     var d = d3.select(this.parentNode).datum();
//
//
//     var radiusSource = this.getClazzConfigVal(this.getClazzName(d.source), "r", null);
//     var radiusTarget = this.getClazzConfigVal(this.getClazzName(d.target), "r", null);
//
//     radiusSource = radiusSource ? radiusSource : this.getConfigVal("node").r;
//     radiusTarget = radiusTarget ? radiusTarget : this.getConfigVal("node").r;
//
//
//     var padd = 5;
//
//     radiusTarget = parseInt(radiusTarget);
//     radiusSource = parseInt(radiusSource);
//     var deltaX = d.target.x - d.source.x,
//       deltaY = d.target.y - d.source.y,
//       dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
//
//
//       normX = deltaX / (dist != 0 ? dist : 1),
//       normY = deltaY / (dist != 0 ? dist : 1),
//       sourcePadding = d.left ? (radiusSource + padd) : radiusSource,
//       targetPadding = d.right ? (radiusTarget + padd) : radiusTarget,
//       sourceX = d.source.x + (sourcePadding * normX),
//       sourceY = d.source.y + (sourcePadding * normY),
//       targetX = d.target.x - (targetPadding * normX),
//       targetY = d.target.y - (targetPadding * normY);
//     // Config Node - > Label
//
//
//     var rel = this.countRelInOut(d);
//
//
//     if (rel == 1) {
//       return 'M' + sourceX + ',' + sourceY + ' L' + targetX + ',' + targetY;
//     } else {
//
//       var realPos = this.calculateRelPos(d);
//
//
//       if (realPos == 0) {
//         var paddingSource = 5;
//         var paddingTarget = 5;
//         if (deltaX > 0) {
//           paddingSource = -1 * 5;
//           paddingTarget = -1 * 5;
//         }
//
//         return 'M' + (sourceX + paddingSource) + ',' + (sourceY + paddingSource) + ' L' + (targetX + paddingTarget) + ',' + (targetY + paddingTarget);
//       }
//       var pos = realPos + 1;
//       var m = (d.target.y - d.source.y) / (d.target.x - d.source.x);
//       var val = (Math.atan(m) * 180) / Math.PI;
//       var trans = val * (Math.PI / 180) * -1;
//       var edgesLength = this.countRel(d);
//       var radiansConfig = this.angleRadiants(pos, edgesLength);
//
//       var angleSource;
//       var angleTarget;
//       var signSourceX;
//       var signSourceY;
//       var signTargetX;
//       var signTargetY;
//
//       if (deltaX < 0) {
//         signSourceX = 1;
//         signSourceY = 1;
//         signTargetX = 1;
//         signTargetY = 1;
//         angleSource = radiansConfig.target - trans;
//         angleTarget = radiansConfig.source - trans;
//       } else {
//         signSourceX = 1;
//         signSourceY = -1;
//         signTargetX = 1;
//         signTargetY = -1;
//         angleSource = radiansConfig.source + trans;
//         angleTarget = radiansConfig.target + trans;
//       }
//
//
//       sourceX = d.source.x + ( signSourceX * (sourcePadding * Math.cos(angleSource)));
//       sourceY = d.source.y + ( signSourceY * (sourcePadding * Math.sin(angleSource)));
//       targetX = d.target.x + ( signTargetX * (targetPadding * Math.cos(angleTarget)));
//       targetY = d.target.y + ( signTargetY * (targetPadding * Math.sin(angleTarget)));
//
//
//       // var mod = dist / 10;
//       // var dr = mod * rel;
//
//       var dr = this.calculateDR(targetX - sourceX, targetY - sourceY, pos, edgesLength);
//
//       return "M" + sourceX + "," + sourceY + "A" + dr + "," + dr + " 0 0,1 " + targetX + "," + targetY;
//     }
//
//   }
//
//
//   calculateDR(dx, dy, pos, length) {
//     pos = length - pos;
//     var dr = Math.sqrt(dx * dx + dy * dy);
//
//     dr = dr / (1 + (1 / length) * (pos - 1));
//
//     return dr;
//
//   }
//
//   angleRadiants(pos, length) {
//
//
//     let sourceAngle = 90 - (90 / length) * pos;
//     let targetAngle = (180 - ( 90 - (90 / length) * pos));
//
//     return {source: sourceAngle * (Math.PI / 180), target: targetAngle * (Math.PI / 180)};
//
//   }
//
//   bindRectColor(d) {
//     var def = "rgba(0, 0, 0, 0)";
//     var c = this.bindColor(d, "displayBackground", def);
//     if (c == "#ffffff") {
//       c = def;
//     }
//     return c;
//   }
//
//   bindName(d) {
//
//     var name = this.getClazzConfigVal(this.getClazzName(d), "icon", null);
//
//     if (!name) {
//       name = this.getClazzConfigVal(this.getClazzName(d), "display", d.source);
//     }
//
//
//     var rid;
//     if (d['@rid'].startsWith("#-")) {
//
//       var props = Object.keys(d.source).filter(function (e) {
//         return !e.startsWith("@");
//       })
//       rid = (props.length > 0 && d.source[props[0]]) ? d.source[props[0]] : d['@rid'];
//     } else {
//       rid = d['@rid'];
//     }
//
//     return name != null ? name : rid;
//   }
//
//   changeClazzConfig(clazz, prop, val) {
//     if (this.changer[prop])
//       this.changer[prop](clazz, prop, val);
//   }
//   changeLinkDistance(distance) {
//     this.force.linkDistance(distance);
//     this.config.linkDistance = distance;
//   }
//
//   getClazzConfigVal(clazz, prop, obj) {
//     if (!clazz || !prop) return null;
//
//
//     if (this.config.classes && this.config.classes[clazz] && this.config.classes[clazz][prop]) {
//       return obj ? obj[this.config.classes[clazz][prop]] : this.config.classes[clazz][prop];
//     }
//     return null;
//   }
//   getConfig() {
//     return this.config;
//   }
//   getClazzConfig(clazz) {
//     if (!this.config.classes) {
//       this.config.classes = {};
//     }
//     if (!this.config.classes[clazz]) {
//       this.config.classes[clazz] = {};
//     }
//     return this.getMerger().extend({}, this.config.classes[clazz]);
//   }
//   getConfigVal(prop) {
//     return this.config[prop];
//   }
//   removeInternalVertex(v) {
//     this.clearSelection();
//     var idx = this.nodes.indexOf(v);
//     this.nodes.splice(idx, 1);
//     var toSplice = this.links.filter(function (l) {
//       return (l.source === v || l.target === v);
//     });
//     toSplice.map(function (l) {
//       this.links.splice(this.links.indexOf(l), 1);
//     });
//
//   }
//   removeInternalEdge(e) {
//     var idx = this.links.indexOf(e);
//     this.links.splice(idx, 1);
//     this.clearSelection();
//
//     //d3-graph.select(e.elem).remove();
//   }
//   zoom() {
//
//     var scale = d3.event.scale;
//     var translation = d3.event.translate;
//     this.svg.attr("transform", "translate(" + translation + ")" +
//       " scale(" + scale + ")");
//   }
//   refreshSelected(change) {
//
//     if (this.selected) {
//       var selR = parseInt(this.bindRadius(this.selected));
//       this.menu.refreshPosition(selR, change);
//
//     }
//   }
//
//   tick() {
//
//     var path = this.path.selectAll("path.edge");
//
//     path.attr('d', this.calculateEdgePath);
//
//     var overlay = this.path.selectAll("path.path-overlay");
//
//     overlay.attr('d', function () {
//       return this.calculateEdgePath.bind(this)(0);
//     });
//
//
//     if (this.edgeMenu) {
//       this.edgeMenu.refreshPosition();
//     }
//     this.circle.attr('transform', function (d) {
//       return 'translate(' + d.x + ',' + d.y + ')';
//     });
//     // refreshSelected();
//   }
//
//   isVertex(elem) {
//     if (typeof elem == 'object') {
//       return !(elem['in'] && elem['out']) && elem['@rid'];
//     } else {
//       var cid = elem.replace("#", "").split(":")[0];
//       var cfg = this.clusterClass[cid];
//
//       if (cfg) return cfg.isVertex;
//     }
//     return false;
//   }
//
// }
