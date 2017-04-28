export class OEdge {

  private graph;
  private source;
  private target;
  private left;
  private right;
  private label;
  private edge;

  constructor(graph, v1, v2, label, edge) {
    this.graph = graph;
    this.source = v1;
    this.target = v2;
    this.left = false;
    this.right = true;
    this.label = label;
    this.edge = edge;
  }

}
