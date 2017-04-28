import '../../../../styles/orientdb-graphviz.css';

import * as d3 from 'd3';

export class OGraph {

  // DOM elements
  private config;
  private originElement;
  private svgContainer;
  private svg;
  private gNodes;
  private gLinks;

  // data
  private vertices;
  private edges;

  // graph elements
  private force;
  private nodes;
  private links;
  private nodeLabels;
  private linkLabels;

  // zooom
  private zoomComponent;
  private minZoom;
  private maxZoom;

  constructor(elemId, config) {

    this.config = config;
    this.originElement = d3.select(elemId);

    this.vertices = [];
    this.edges = [];

    this.minZoom = 0.1;
    this.maxZoom = 10;
  }

  data(data) {

    if(data) {

      if (data.vertices) {
        this.vertices = data.vertices;
      }
      if (data.edges) {
        this.edges = data.edges;
      }

    }

    for(var edge of this.edges) {
      var edgeName = Object.keys(edge)[0];
      var fromTableName = edge[edgeName].mapping[0].fromTable;
      var toTableName = edge[edgeName].mapping[0].toTable;

      var fromVertexClass = this.getVertexClassBySourceTableName(fromTableName);
      var toVertexClass = this.getVertexClassBySourceTableName(toTableName);

      edge.source = fromVertexClass;
      edge.target = toVertexClass;
    }

    for(var edge of this.edges) {
      console.log("Edge: " + Object.keys(edge));
      console.log("Edge source: "+ edge.source.name);
      console.log("Edge target: "+ edge.target.name);
      console.log("");
    }

    return this;
  }

  initLayout() {

    this.force = d3.layout.force()
      .size([this.config.width, this.config.height])
      .linkStrength(0.1)
      .charge(this.config.charge)
      .friction(this.config.friction)
      .linkDistance(this.config.linkDistance)
      .on("tick", () => {this.tick()});

    var drag = this.force.drag()
      .on("dragstart", dragstart);

    // preparing svg
    this.svgContainer = this.originElement.append("svg")
      .attr("width", this.config.width)
      .attr("height", this.config.height)
      .attr("pointer-events", "all");
    this.svg = this.svgContainer
      .append("g");

    this.svg.append('svg:rect')
      .attr('width', this.config.width)
      .attr('height', this.config.height)
      .attr('fill', 'white');


    this.gNodes = this.svg.append("g")
      .attr("class","g-nodes");

    this.gLinks = this.svg.append("g")
      .attr("class","g-links");

    // define arrow markers for graph links
    this.svgContainer.append('svg:defs').append('svg:marker')
      .attr('id', 'end-arrow')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 6)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('svg:path')
      .attr('d', 'M0,-5L10,0L0,5')
      .attr('fill', '#000')
      .attr('class', 'end-arrow');

    this.svgContainer.append('svg:defs').append('svg:marker')
      .attr('id', 'start-arrow')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 4)
      .attr('markerWidth', 3)
      .attr('markerHeight', 3)
      .attr('orient', 'auto')
      .append('svg:path')
      .attr('d', 'M10,-5L0,0L10,5')
      .attr('fill', '#000')
      .attr('class', 'end-arrow');

    // define arrow markers for graph links
    this.svgContainer.append('svg:defs').append('svg:marker')
      .attr('id', 'end-arrow-hover')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 6)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('svg:path')
      .attr('d', 'M0,-5L10,0L0,5')
      .attr('class', 'end-arrow-hover');

    this.svgContainer.append('svg:defs').append('svg:marker')
      .attr('id', 'start-arrow-hover')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 4)
      .attr('markerWidth', 3)
      .attr('markerHeight', 3)
      .attr('orient', 'auto')
      .append('svg:path')
      .attr('d', 'M10,-5L0,0L10,5')
      .attr('fill', '#000')
      .attr('class', 'end-arrow-hover');

    // selecting then setting vertices and edges in the force layout
    this.links = this.gLinks.selectAll(".link");
    this.nodes = this.gNodes.selectAll(".node");
    this.force
      .nodes(this.vertices)
      .links(this.edges);

    // setting graph elements

    this.nodes = this.nodes
      .data(this.vertices)
      .enter().append("circle")
      .attr("class", "node")
      .attr("r", this.config.node.r)
      .attr("stroke", "#0066cc")
      .attr("stroke-width", "1px")
      .attr("fill", '#99ccff')
      .on("dblclick", dblclick)
      .call(drag);

    this.nodeLabels = this.gNodes.selectAll(".node-label")
      .data(this.vertices)
      .enter().append("svg:text")
      .attr("class", "node-label")
      .attr("dx", -1.5 * this.config.node.r)
      .attr("dy", -1.5 * this.config.node.r)
      .text(function(d) { return d.name; });

    this.links = this.links
      .data(this.edges, function (d) {return Object.keys(d)[0];})
      .enter().append("path")
      .attr("id", function(d) {return Object.keys(d)[0];})
      .attr("class", "link")
      .attr("stroke", "#000")
      .attr("stroke-width", "1.5px")
      .style("marker-end", "url(#end-arrow)");

    this.linkLabels = this.gLinks.selectAll(".link-label")
      .data(this.edges, function (d) {return Object.keys(d)[0];})
      .enter().append('text')
      .style("text-anchor", "middle")
      .attr("dy", "-8")
      .attr("class","link-label")
      .append("svg:textPath")
      .attr("startOffset", "50%")
      // .attr("text-anchor", "middle")
      .attr("xlink:href", function(d) { return "#" + Object.keys(d)[0]; })
      .attr("baselineShift", "super")
      .style("fill", "black")
      // .style("font-family", "Arial")
      .text(function(d) { return Object.keys(d)[0]; });

    // zoom
    this.zoomComponent = d3.behavior.zoom()
      .scaleExtent([this.minZoom, this.maxZoom])
      .on("zoom", () => {this.zoom()});

    this.svg.call(this.zoomComponent);

    function dblclick(d) {
      d3.select(this).classed("fixed", d.fixed = false);
    }

    function dragstart(d) {
      d3.select(this).classed("fixed", d.fixed = true);
    }

  }

  tick() {

    var r = this.config.node.r;
    var arrivalRadius = r + 4;  // 4 added because we must take into account the refX of the marker (end arrow of the link)

    this.links.attr("d", function(d) {

      // Total difference in x and y from source to target
      var diffX = d.target.x - d.source.x;
      var diffY = d.target.y - d.source.y;

      // Length of path from the edge of source node to edge of target node
      var pathLength = Math.sqrt((diffX * diffX) + (diffY * diffY));

      // x and y distances from center to outside edge of source node
      var sourceOffsetX = (diffX * r) / pathLength;
      var sourceOffsetY = (diffY * r) / pathLength;

      // x and y distances from center to outside edge of target node
      var targetOffsetX = (diffX * arrivalRadius) / pathLength;
      var targetOffsetY = (diffY * arrivalRadius) / pathLength;

      return "M" + (d.source.x + sourceOffsetX) + "," + (d.source.y + sourceOffsetY) +
        "L" + (d.target.x - targetOffsetX) + "," + (d.target.y - targetOffsetY);
    });

    this.nodes.attr("transform", this.transform);
    this.nodeLabels.attr("transform", this.transform);

  }

  transform(d) {
    return "translate(" + d.x + "," + d.y + ")";
  }

// zoom g
  zoom() {
    var scale = d3.event.scale;
    var translation = d3.event.translate;
    this.svg.attr("transform", "translate(" + translation + ")" +
      " scale(" + scale + ")");
  }


  draw() {
    console.log('Drawing the graph.')
    this.initLayout();
    this.force.start();
  }


  getVertexClassBySourceTableName(tableName) {

    for(var vertexClass of this.vertices) {

      var firstTableMapped = vertexClass.mapping.sourceTables[0];  // only with aggregation we have more than one table mapped
      if(firstTableMapped && firstTableMapped.tableName === tableName) {
        return vertexClass;
      }
    }
    return null;
  }



}
