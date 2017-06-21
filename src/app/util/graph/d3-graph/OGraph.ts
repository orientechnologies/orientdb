import "../../../../styles/orientdb-graphviz.css";
import * as d3 from "d3";

export class OGraph {

  private graphContainer;

  // DOM elements
  private config;
  private originElement;
  private svg;
  private innerContainer;
  private legend;
  private radiusLegendNode;
  private gNodes;
  private gLinks;
  private nodeContainers;
  private linkContainers;

  // data
  private vertices;
  private edges;

  // graph elements
  private force;
  private nodes;
  private links;
  private nodeLabels;
  private linkLabels;
  private dragLine;

  // zoom and drag
  private zoomComponent;
  private minZoom;
  private maxZoom;
  private drag;

  // selection and highlighting
  private highlightColor;
  private highlightTrans;
  private selectedElement;
  private startingNode;
  private arrivalNode;


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

      // if other mappings are defined will be used to generate other edges definitions, that will be added to this.edges
      var truncateMappings = false;
      for(var j=1; j<edge[edgeName].mapping.length; j++) {
        var currEdge = {};
        truncateMappings = true;

        var edgeDef = {mapping: [], properties: {}, isLogical: false};
        edgeDef.mapping[0] = edge[edgeName].mapping[j];
        edgeDef.properties = edge[edgeName].properties;
        edgeDef.isLogical = edge[edgeName].isLogical;

        currEdge[edgeName] = edgeDef;
        this.edges.push(currEdge);
      }

      if(truncateMappings) {
        edge[edgeName].mapping = edge[edgeName].mapping.slice(0, 1);
      }
    }

    return this;
  }

  initLayout() {

    // preparing svg
    this.svg = this.originElement.append("svg")
      .attr("width", "100%")
      .attr("height", "100%")
      .style("cursor", "move")
      .on("click", () => {
        this.deselectLastElement();
        if(this.startingNode) {
          // openAddEdgeModal edge creation
          this.openEdgeCreationModal();
        }
      })
      .on('mousemove', () => {
        if(this.startingNode) {
          this.moveDragLine();
        }
      });

    this.innerContainer = this.svg
      .append("g");

    // line displayed when dragging a new edge between two nodes (order relevant, in this way it will always come before gNodes and gLinks)
    this.dragLine = this.innerContainer
      .append("g")
      .attr("class", "g-drag-line")
      .append('path')
      .attr('class', 'link hidden')
      .attr('d', 'M0,0L0,0');

    this.gNodes = this.innerContainer.append("g")
      .attr("class", "g-nodes")
      .style("cursor", "pointer");

    this.gLinks = this.innerContainer.append("g")
      .attr("class", "g-links")
      .style("cursor", "pointer");


    // Legend
    this.radiusLegendNode = this.config.node.r/2;
    this.legend = this.svg.append('g')
      .attr("class", "graph-legend")
      .attr("transform","translate(20,20)")
      .style("font-size","12px");

    this.legend.append("circle")
      .attr("id", "node-icon")
      .attr("class", "node-icon")
      .attr("r", this.config.node.r/2)
      .attr("stroke", "#0066cc")
      .attr("stroke-width", "1px")
      .attr("fill", "#99ccff");
    this.legend.append("svg:text")
      .attr("id", "node-icon-label")
      .attr("class", "node-icon-label")
      .attr("dx", +1.5 * this.radiusLegendNode)
      .attr("dy", this.radiusLegendNode/2)
      .text("Vertex Class");

    this.legend.append("path")
      .attr("class", "link-icon")
      .attr("d", "M-10,25L30,25")
      .attr("stroke", "#000")
      .attr("stroke-width", "1.5px")
      .style("marker-end", "url(#end-arrow)");
    this.legend.append("svg:text")
      .attr("id", "link-icon-label")
      .attr("class", "link-icon-label")
      .attr("dx", 40)
      .attr("dy", 30)
      .text("Edge Class from");
    this.legend.append("svg:text")
      .attr("id", "link-icon-label")
      .attr("class", "link-icon-label")
      .attr("dx", 40)
      .attr("dy", 50)
      .text("1-N Relationship");

    this.legend.append("path")
      .attr("class", "link-icon")
      .attr("d", "M-10,65L30,65")
      .attr("stroke", "#000")
      .attr("stroke-dasharray", "5")
      .attr("stroke-width", "1.5px")
      .style("marker-end", "url(#end-arrow)");
    this.legend.append("svg:text")
      .attr("id", "link-icon-label")
      .attr("class", "link-icon-label")
      .attr("dx", 40)
      .attr("dy", 70)
      .text("Edge Class from");
    this.legend.append("svg:text")
      .attr("id", "link-icon-label")
      .attr("class", "link-icon-label")
      .attr("dx", 40)
      .attr("dy", 90)
      .text("N-N Relationship");

    // legend hidden by default
    this.legend.attr("opacity", 0);

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

    // Force layout setting

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

    this.force = d3.layout.force()
      .size([this.config.width, this.config.height])
      .linkStrength(this.config.linkStrength)
      .charge(this.config.charge)
      .friction(this.config.friction)
      .linkDistance(this.config.linkDistance)
      .on("tick", () => {this.tick()});

    this.drag = this.force.drag()
      .on("dragstart", dragstart)
      // .on("drag", dragging)
      .on("dragend", dragend);

    // setting graph elements

    // gnodes and glinks
    this.gNodes = d3.select(".g-nodes");
    this.gLinks = d3.select(".g-links");

    this.nodeContainers = this.gNodes.selectAll(".node-container");
    this.linkContainers = this.gLinks.selectAll(".link-container");

    this.initGraphElements();
    this.setBehavioursForNodesAndLinks();

    // zoom
    this.zoomComponent = d3.behavior.zoom()
      .scaleExtent([this.minZoom, this.maxZoom])
      .on("zoom", () => {this.zoom()});
    this.svg
      .call(this.zoomComponent);
  }

  // zoom the 'g' inner container
  zoom() {
    var scale = d3.event.scale;
    var translation = d3.event.translate;
    this.innerContainer.attr("transform", "translate(" + translation + ")" +
      " scale(" + scale + ")");
  }

  /**
   * Adds/Removes in the svg all the elements (nodes and links with correspondent labels) according to entering/exiting data.
   */
  initGraphElements() {

    this.force
      .nodes(this.vertices)
      .links(this.edges);

    /*
     * Nodes
     */

    // entering nodes

    this.nodeContainers = this.nodeContainers
      .data(this.vertices, function(d) { return d.name});

    var enteringNodeContainers = this.nodeContainers
      .enter().append("g")
      .attr("id", function(d) {return "node-container-" + d.name;})
      .attr("class", "node-container");

    enteringNodeContainers
      .append("circle")
      .attr("id", function(d) {return d.name})
      .attr("class", "node")
      .attr("r", this.config.node.r)
      .attr("stroke", "#0066cc")
      .attr("stroke-width", "1px")
      .attr("fill", "#99ccff");

    this.nodes = this.gNodes.selectAll(".node");

    enteringNodeContainers
      .append("svg:text")
      .attr("id", function(d) {return "vlabel-" + d.name;})
      .attr("class", "node-label")
      .attr("dx", -1.5 * this.config.node.r)
      .attr("dy", -1.5 * this.config.node.r)
      .text(function(d) { return d.name; });

    this.nodeLabels = this.gNodes.selectAll(".node-label");

    // exiting nodes
    this.nodeContainers
      .exit().remove();


    /*
     * Links
     */

    // entering links

    this.linkContainers = this.linkContainers
      .data(this.edges, (d) => {return this.getEdgeClassName(d) + "__" + d.source.name + "--" + d.target.name;});

    var enteringLinkContainers = this.linkContainers
      .enter().append("g")
      .attr("id", (d) => {return "link-container-" + this.getEdgeClassName(d);})
      .attr("class", "link-container");

    enteringLinkContainers
      .append("path")
      .attr("id", (d) => {return this.getEdgeClassName(d) + "__" + d.source.name + "--" + d.target.name;})
      .attr("class", "link")
      .attr("stroke", "#000")
      .attr("stroke-dasharray", (d) => {
        var edgeName = this.getEdgeClassName(d);
        if(d[edgeName].mapping && d[edgeName].mapping[0].joinTable) {
          return "5";
        }
        else {
          return "";
        }
      })
      .attr("stroke-width", "1.5px")
      .style("marker-end", "url(#end-arrow)");

    this.links = this.gLinks.selectAll(".link")

    enteringLinkContainers
      .append('text')
      .attr("class", "link-label")
      .style("text-anchor", "middle")
      .attr("dy", "-8")
      .append("svg:textPath")
      .attr("id", (d) => {return "llabel-" + this.getEdgeClassName(d) + "__" + d.source.name + "--" + d.target.name;})
      .attr("class", (d) => {return "llabel-" + this.getEdgeClassName(d);})
      .attr("startOffset", "50%")
      .attr("xlink:href", (d) => { return "#" + this.getEdgeClassName(d) + "__" + d.source.name + "--" + d.target.name; })
      .attr("baselineShift", "super")
      .style("fill", "black")
      // .style("font-family", "Arial")
      .text((d) => {return this.getEdgeClassName(d);});

    this.linkLabels = this.gLinks.selectAll(".link-label");

    // exiting links
    this.linkContainers
      .exit().remove();
  }


  setBehavioursForNodesAndLinks() {

    var self = this;

    function dblclick(d) {
      d3.select(this).classed("fixed", d.fixed = false);
    }

    function highlightElement(d) {

      self.nodes.style("opacity", 0.1);
      self.links.style("opacity", 0.1);
      self.nodeLabels.style("opacity", 0.1);
      self.linkLabels.style("opacity", 0.1);

      // highlighting elements

      if(d.name) {
        d3.select("#" + d.name)
          .style("opacity", 1);
        d3.select("#vlabel-" + d.name)
          .style("opacity", 1);
      }
      else {
        var edgeClassName = self.getEdgeClassName(d);

        d3.selectAll("#" + edgeClassName + "__" + d.source.name + "--" + d.target.name)
          .style("opacity", 1);
        d3.selectAll("#llabel-" + edgeClassName + "__" + d.source.name + "--" + d.target.name)
          .select(function() {
            return this.parentNode;
          })
          .style("opacity", 1);
      }
    }

    function clearElementHighlight(d) {

      self.nodes.style("opacity", 1);
      self.links.style("opacity", 1);
      self.nodeLabels.style("opacity", 1);
      self.linkLabels.style("opacity", 1);
    }

    // highlighting and selection

    this.nodeContainers
      .on("mouseover", highlightElement)
      .on("mouseout", clearElementHighlight)
      .call(this.drag);

    this.linkContainers
      .on("mouseover", highlightElement)
      .on("mouseout", clearElementHighlight);

    this.nodes
      .on("click", (d) => {
        // stopping higher events
        d3.event.stopPropagation();

        if(this.startingNode) {
          // setting arrivalNode
          this.arrivalNode = d;

          // openAddEdgeModal edge creation
          this.openEdgeCreationModal();
        }
        this.selectNode(d);
      })
      .on("dblclick", dblclick);

    this.nodeLabels
      .on("click", (d) => {
        // stopping higher events
        d3.event.stopPropagation();
        this.selectNode(d)});

    this.links
      .on("click", (d) => {
        // stopping higher events
        d3.event.stopPropagation();
        this.selectLink(d)});

    this.linkLabels
      .on("click", (d) => {
        // stopping higher events
        d3.event.stopPropagation();
        this.selectLink(d)});
  }

  tick() {

    var r = this.config.node.r;
    var arrivalRadius = r + 4;  // 4 added because we must take into account the refX of the marker (end arrow of the link)

    // links' function
    this.linkContainers
      .selectAll("path")
      .attr("d", function(d) {

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

    // nodes' function
    this.nodeContainers.attr("transform", this.transform);

  }

  transform(d) {
    if(!d) {
      console.log("Fuck!");
    }
    return "translate(" + d.x + "," + d.y + ")";
  }

  draw() {
    this.initLayout();
    this.force.start();
  }

  redraw() {

    this.initGraphElements();
    this.setBehavioursForNodesAndLinks();
    this.force.start();
  }

  stop() {
    this.force.stop();
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
    this.graphContainer.setSelectedElement(element);    // sends the selected element to the graph container in order to generate an appropriate event.
  }

  selectNode(node) {

    // deselect previous selected element
    this.deselectLastElement();

    // selection
    this.setSelectedElement(node);

    d3.select("#" + node.name)
      .attr("stroke", "#bfff00")
      .attr("stroke-width", "5px");

    // highlighting the label
    d3.select("#vlabel-" + node.name)
      .style("fill", "cc0000")
      .style("font-weight", "bold");

  }

  selectLink(link) {

    // deselect previous selected element
    this.deselectLastElement();

    // selection
    this.setSelectedElement(link);

    // performing this check because during the execution the keys sorting can change
    var edgeClassName = this.getEdgeClassName(link);

    d3.select("#" + edgeClassName + "__" + link.source.name + "--" + link.target.name)
      .attr("stroke", "#bfff00")
      .attr("stroke-width", "2px");

    // highlighting the label
    d3.selectAll(".llabel-" + edgeClassName)
      .style("fill", "cc0000")    // red
      .style("font-weight", "bold");

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
    d3.select("#vlabel-" + node.name)
      .style("fill", "black")
      .style("font-weight", "normal");

  }

  deselectLink(link) {

    this.setSelectedElement(undefined);

    // performing this check because during the execution the keys sorting can change
    var edgeClassName = this.getEdgeClassName(link);

    d3.select("#" + edgeClassName + "__" + link.source.name + "--" + link.target.name)
      .attr("stroke", "#000")
      .attr("stroke-width", "1px");

    // highlighting the label
    d3.selectAll(".llabel-" + edgeClassName)
      .style("fill", "black")
      .style("font-weight", "normal");

  }

  getEdgeClassName(link) {

    var edgeClassName = undefined;
    var keys = Object.keys(link);

    for(var key of keys) {
      if(key !== 'source' && key !== 'target') {
        edgeClassName = key;
        break;
      }
    }

    return edgeClassName;
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

  updateElementsAccordingToRenamedClassed(oldClassName, newClassName, classType) {

    if(classType === 'vertexClass') {

      // update vertex
      d3.select("#" + oldClassName)
        .attr("id", newClassName);

      // update vertex label
      d3.select("#vlabel-" + oldClassName)
        .attr("id", "vlabel-" + newClassName)
        .attr("xlink:href", "#" + newClassName)
        .text(newClassName);

      // update ids of connected edges and their labels as they are based on the vertex name
      var edgeClassName = undefined;
      var updatedEdgesNames = [];
      for(var edge of this.edges) {
        if(edge.source.name === newClassName) {
          edgeClassName = this.getEdgeClassName(edge);
          if(updatedEdgesNames.indexOf(edgeClassName) === -1) {
            // if the edge class is not updated yet, update all the connected elements in the svg,
            // then add the edge class name to the list
            this.updateEdgeIds(edgeClassName);
            updatedEdgesNames.push(edgeClassName);
          }
        }
        else if(edge.target.name === newClassName) {
          edgeClassName = this.getEdgeClassName(edge);
          if(updatedEdgesNames.indexOf(edgeClassName) === -1) {
            // if the edge class is not updated yet, update all the connected elements in the svg,
            // then add the edge class name to the list
            this.updateEdgeIds(edgeClassName);
            updatedEdgesNames.push(edgeClassName);
          }
        }
      }
    }
    else if(classType === 'edgeClass') {

      // update edges

      d3.selectAll("#link-container-" + oldClassName)
        .attr("id", "link-container-" + newClassName)
        .select("path")
        .attr("id", (d) => {return newClassName + "__" + d.source.name + "--" + d.target.name;})


      // update edge labels
      d3.selectAll(".llabel-" + oldClassName)
        .attr("class", "llabel-" + newClassName)
        .attr("id", function(d) { return "llabel-" + newClassName + "__" + d.source.name + "--" + d.target.name;})
        .attr("xlink:href", function(d) { return "#" + newClassName + "__" + d.source.name + "--" + d.target.name;})
        .text(newClassName);
    }
  }

  updateEdgeIds(edgeClassName) {

    // update edges
    d3.selectAll("#link-container-" + edgeClassName)
      .select("path")
      .attr("id", function(d) { return edgeClassName + "__" + d.source.name + "--" + d.target.name;});

    // update edge labels
    d3.selectAll(".llabel-" + edgeClassName)
      .attr("id", function(d) { return "llabel-" + edgeClassName + "__" + d.source.name + "--" + d.target.name;})
      .attr("xlink:href", function(d) { return "#" + edgeClassName + "__" + d.source.name + "--" + d.target.name;});
  }

  removeClass(className, classType) {

    if(classType === 'vertexClass') {

      // removing the vertex class from this.edges
      for(var i=0; i<this.vertices.length; i++) {
        if(this.vertices[i].name === className) {
          this.vertices.splice(i, 1);
          break;
        }
      }
    }
    else  if(classType === 'edgeClass') {

      // removing the edge class from this.edges
      for(var i=0; i<this.edges.length; i++) {
        var edge = this.edges[i];
        if(this.getEdgeClassName(edge) === className) {
          this.edges.splice(i, 1);
          i--;
          // no break so we can remove all the other instances, if present, of the specific class.
        }
      }
    }

    // redraw
    this.redraw();
  }

  removeEdgeInstance(edgeClassName, sourceName, targetName) {

    // deselect all the instances of the edge
    this.deselectLastElement();

    for(var i=0; i<this.edges.length; i++) {
      var edge = this.edges[i];
      if(this.getEdgeClassName(edge) === edgeClassName && edge.source.name === sourceName && edge.target.name === targetName) {
        this.edges.splice(i, 1);
        break;    // other matching items are impossible by design
      }
    }

    // redraw
    this.redraw();
  }

  startEdgeCreation() {

    this.startingNode = this.selectedElement;
    this.deselectLastElement();

    // reposition drag line
    this.dragLine
      .style('marker-end', 'url(#end-arrow)')
      .classed('hidden', false)
      .attr('d', 'M' + this.startingNode.x + ',' + this.startingNode.y + 'L' + this.startingNode.x + ',' + this.startingNode.y)
      .attr("stroke", "#000")
      .attr("stroke-width", "1.5px");
  }

  openEdgeCreationModal() {
    this.deselectLastElement();

    // ***********************************************************************
    // proof --> TO DELETE
    // this.edges.length = 0;
    // var edge = {
    //   "has_subtitle":{
    //     "mapping":[
    //       {
    //         "fromColumns":[
    //           "sub_id"
    //         ],
    //         "toTable":"subtitle",
    //         "toColumns":[
    //           "sub_id"
    //         ],
    //         "fromTable":"language",
    //         "direction":"direct"
    //       }
    //     ],
    //     "isLogical":false,
    //     "properties":{
    //
    //     }
    //   },
    //   "source": this.getVertexClassBySourceTableName("language"),
    //   "target": this.getVertexClassBySourceTableName("subtitle")
    // };
    // this.edges.push(edge);

    // var newVertex = {
    //   "externalKey":[
    //     "newNode_id"
    //   ],
    //   "mapping":{
    //     "sourceTables":[
    //       {
    //         "name":"PostgreSQL_subtitle",
    //         "dataSource":"PostgreSQL",
    //         "tableName":"newNode",
    //         "primaryKey":[
    //           "newNode_id"
    //         ]
    //       }
    //     ]
    //   },
    //   "name":"newNode",
    //   "properties":{},
    //   "px": 295.4866397619178,
    //   "py": -15.98558877972299,
    //   "x": 295.4866397619178,
    //   "y": -15.98558877972299
    // }
    // this.vertices.push(newVertex);

    // ***********************************************************************

    if(this.arrivalNode) {

      // starting modal
      var edgeClassesNames = [];
      for (var i = 0; i < this.edges.length; i++) {
        var currEdgeName = this.getEdgeClassName(this.edges[i]);
        if (edgeClassesNames.indexOf(currEdgeName) === -1) {
          edgeClassesNames.push(currEdgeName);
        }
      }
      var fromVertex = JSON.parse(JSON.stringify(this.startingNode));
      var toVertex = JSON.parse(JSON.stringify(this.arrivalNode));
      this.graphContainer.openEdgeCreationModal(this.startingNode, this.arrivalNode, edgeClassesNames);
    }
    else {
      this.clearArrow();
    }
  }

  endEdgeCreation(newEdge) {

    this.clearArrow();

    if (newEdge) {

      // add the new edge and redraw the graph
      this.addEdge(newEdge);
      this.redraw();
    }
  }

  /**
   * Drops the arrow used to define the starting and the arrival node, then it cleans startingNode and arrivalNode.
   */
  clearArrow() {

    if (this.startingNode) {
      this.startingNode = undefined;
      this.dragLine
        .classed('hidden', true)
        .style('marker-end', '');
    }
    if(this.arrivalNode) {
      this.arrivalNode = undefined;
    }
  }

  /**
   * Adds the new edge passed as param.
   * @param newEdge
   */
  addEdge(newEdge) {
    this.edges.push(newEdge);
  }

  /**
   * Moves the drag line if the edge creation procedure is happening
   */
  moveDragLine() {
    // update drag line
    this.dragLine.attr('d', 'M' + this.startingNode.x + ',' + this.startingNode.y + 'L' + d3.mouse(this.innerContainer.node())[0] + ',' + d3.mouse(this.innerContainer.node())[1]);  }

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
