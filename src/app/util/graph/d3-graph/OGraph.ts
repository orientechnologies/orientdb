import '../../../../styles/orientdb-graphviz.css';


import * as d3 from 'd3';

export class OGraph {

  private graphContainer;

  // DOM elements
  private config;
  private originElement;
  private svg;
  private innerContainer;
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

  // zoom
  private zoomComponent;
  private minZoom;
  private maxZoom;

  // selection and highlighting
  private highlightColor;
  private highlightTrans;
  private selectedElement;


  constructor(elemId, config, container) {

    this.graphContainer = container;

    this.config = config;
    this.originElement = d3.select(elemId);

    this.vertices = [];
    this.edges = [];

    this.minZoom = 0.1;
    this.maxZoom = 10;

    this.highlightColor = "blue";
    this.highlightTrans = 0.1;
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

    return this;
  }

  initLayout() {

    var self = this;

    var margin = {top: -5, right: -5, bottom: -5, left: -5};

    this.force = d3.layout.force()
      .size([this.config.width, this.config.height])
      .linkStrength(0.1)
      .charge(this.config.charge)
      .friction(this.config.friction)
      .linkDistance(this.config.linkDistance)
      .on("tick", () => {this.tick()});

    var drag = this.force.drag()
      .on("dragstart", dragstart)
      // .on("drag", dragging)
      .on("dragend", dragend);

    // preparing svg
    this.svg = this.originElement.append("svg")
      .attr("width", this.config.width)
      .attr("height", this.config.height)
      .style("cursor", "move")
      .on("click", () => {this.deselectLastElement()});
    this.innerContainer = this.svg
      .append("g");

    this.gNodes = this.innerContainer.append("g")
      .attr("class", "g-nodes")
      .style("cursor", "pointer");

    this.gLinks = this.innerContainer.append("g")
      .attr("class", "g-links")
      .style("cursor", "pointer");

    // define arrow markers for graph links
    this.svg.append('svg:defs').append('svg:marker')
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

    this.svg.append('svg:defs').append('svg:marker')
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
    this.svg.append('svg:defs').append('svg:marker')
      .attr('id', 'end-arrow-hover')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 6)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('svg:path')
      .attr('d', 'M0,-5L10,0L0,5')
      .attr('class', 'end-arrow-hover');

    this.svg.append('svg:defs').append('svg:marker')
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
      .enter().append("g")
      .attr("class", "node-container")
      .append("circle")
      .attr("id", function(d) {return d.name})
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
      .attr("id", function(d) {return "vlabel-" + d.name;})
      .attr("class", "node")
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
      .append("svg:textPath")
      .attr("startOffset", "50%")
      .attr("id",function(d) {return "llabel-" + Object.keys(d)[0];})
      .attr("class", "link")
      // .attr("text-anchor", "middle")
      .attr("xlink:href", function(d) { return "#" + Object.keys(d)[0]; })
      .attr("baselineShift", "super")
      .style("fill", "black")
      // .style("font-family", "Arial")
      .text(function(d) { return Object.keys(d)[0]; });

    // highlighting and selection
    this.nodes.on("mouseover", highlightElement);
    this.nodeLabels.on("mouseover", highlightElement);
    this.links.on("mouseover", highlightElement);
    this.linkLabels.on("mouseover", highlightElement);

    this.nodes.on("mouseout", clearElementHighlight);
    this.nodeLabels.on("mouseout", clearElementHighlight);
    this.links.on("mouseout", clearElementHighlight);
    this.linkLabels.on("mouseout", clearElementHighlight);

    this.nodes.on("click", (d) => {
      // stopping higher events
      d3.event.stopPropagation();
      this.selectNode(d)});
    this.nodeLabels.on("click", (d) => {
      // stopping higher events
      d3.event.stopPropagation();
      this.selectNode(d)});
    this.links.on("click", (d) => {
      // stopping higher events
      d3.event.stopPropagation();
      this.selectLink(d)});
    this.linkLabels.on("click", (d) => {
      // stopping higher events
      d3.event.stopPropagation();
      this.selectLink(d)});

    // zoom
    this.zoomComponent = d3.behavior.zoom()
      .scaleExtent([this.minZoom, this.maxZoom])
      .on("zoom", () => {this.zoom()});
    this.svg
      .call(this.zoomComponent);

    function dblclick(d) {
      d3.select(this).classed("fixed", d.fixed = false);
    }

    function dragstart(v) {
      d3.event.sourceEvent.stopPropagation();
      d3.select(this).classed("dragging", true);
      d3.select(this).classed("fixed", v.fixed = true);
    }

    function dragging(d) {
      d.style("cursor", "grabbing");
      d3.select(this).attr("cx", d.x = d3.event.x).attr("cy", d.y = d3.event.y);
    }

    function dragend(v) {
      d3.event.sourceEvent.stopPropagation();
      d3.select(this).classed("dragging", false);
    }

    function highlightElement(d) {

      self.nodes.style("opacity", 0.1);
      self.links.style("opacity", 0.1);
      self.nodeLabels.style("opacity", 0.1);
      self.linkLabels.style("opacity", 0.1);

      // highlighting elements
      d3.select("#" + d.name).style("opacity", 1);
      d3.select("#vlabel-" + d.name).style("opacity", 1);
      d3.select("#" + Object.keys(d)[0]).style("opacity", 1);
      d3.select("#llabel-" + Object.keys(d)[0]).style("opacity", 1);

    }

    function clearElementHighlight(d) {

      self.nodes.style("opacity", 1);
      self.links.style("opacity", 1);
      self.nodeLabels.style("opacity", 1);
      self.linkLabels.style("opacity", 1);
    }
  }

  // zoom the 'g' inner container
  zoom() {
    var scale = d3.event.scale;
    var translation = d3.event.translate;
    this.innerContainer.attr("transform", "translate(" + translation + ")" +
      " scale(" + scale + ")");
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

  draw() {
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

  setSelectedElement(element) {
    this.selectedElement = element;
    this.graphContainer.setSelectedElement(element);
  }

  selectNode(node) {

    // deselect previous selected element
    this.deselectLastElement();

    // selection
    this.setSelectedElement(node);
    // this.selectedElement = node;

    d3.select("#" + node.name)
      .attr("stroke", "#bfff00")
      .attr("stroke-width", "5px");

    // highlighting the label
    d3.select("#vlabel-" + node.name).style("font-weight", "bold");

  }

  selectLink(link) {

    // deselect previous selected element
    this.deselectLastElement();

    // selection
    this.setSelectedElement(link);
    // this.selectedElement = link;

    d3.select("#" + Object.keys(link)[0])
      .attr("stroke", "#bfff00")
      .attr("stroke-width", "2px");

    // highlighting the label
    d3.select("#llabel-" + Object.keys(link)[0]).style("font-weight", "bold");

  }

  deselectLastElement() {

    if(this.selectedElement) {
      if(this.selectedElement.name) { // is a vertex
        this.deselectNode(this.selectedElement);
      }
      else {
        this.deselectLink(this.selectedElement)
      }
    }
  }

  deselectNode(node) {

    this.setSelectedElement(undefined);
    // this.selectedElement = undefined;

    d3.select("#" + node.name)
      .attr("stroke", "#0066cc")
      .attr("stroke-width", "1px");

    // highlighting the label
    d3.select("#vlabel-" + node.name).style("font-weight", "normal");

  }

  deselectLink(link) {

    this.setSelectedElement(undefined);
    // this.selectedElement = undefined;

    d3.select("#" + Object.keys(link)[0])
      .attr("stroke", "#000")
      .attr("stroke-width", "1px");

    // highlighting the label
    d3.select("#llabel-" + Object.keys(link)[0]).style("font-weight", "normal");

  }

  // it performs a search according to the value of the form looking for among the vertices and tables names.
  searchNode(targetName) {

    var matchingNode;

    for(var i=0; i<this.vertices.length; i++) {

      // checking the vertex class name
      if(this.vertices[i].name === targetName) {
        matchingNode = this.vertices[i];
        break;
      }

      // check the correspondent source table name
      else {

        for(var j=0; j<this.vertices[i].mapping.sourceTables.length; j++) {
          var tableName = this.vertices[i].mapping.sourceTables[j].tableName;
          if(tableName === targetName) {
            matchingNode = this.vertices[i];
            break;
          }
        }
      }
    }

    if(matchingNode) {
      this.selectNode(matchingNode);

      // enlarge node selection
      d3.select("#" + matchingNode.name)
        .attr("stroke", "#bfff00")
        .attr("stroke-width", "50px");

      // making selection small again
      d3.select("#" + matchingNode.name).transition()
        .duration(500)
        .attr("stroke", "#bfff00")
        .attr("stroke-width", "5px");
    }

    return matchingNode;
  }

  fadeOutInAllExceptNode(node) {

    var link = this.svg.selectAll(".link")
    link.style("opacity", "0");

    var nodes = this.svg.selectAll(".node").filter(function(d) {
      return d.name != node.name;
    });
    nodes.style("opacity", "0");

    // fading out and fading in all other elements in a short transition
    d3.selectAll(".node, .link").transition()
      .duration(1000)
      .style("opacity", 1);
  }

}
