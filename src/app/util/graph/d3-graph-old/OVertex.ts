export class OVertex {

  private loaded;
  private source;
  private graph;

  constructor(graph, elem) {
    if (elem instanceof Object) {
      this["@rid"] = elem['@rid'];
      this.loaded = true;
    } else {
      this["@rid"] = elem;
      this.loaded = false;
    }
    this.source = elem;
    this.graph = graph;
  }

}
