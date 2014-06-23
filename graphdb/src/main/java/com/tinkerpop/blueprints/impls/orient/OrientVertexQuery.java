package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.util.DefaultVertexQuery;

/**
 * OrientDB implementation for vertex centric queries.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientVertexQuery extends DefaultVertexQuery {

  public OrientVertexQuery(final OrientVertex vertex) {
    super(vertex);
  }

  /**
   * (Blueprints Extension) Counts the total items found. This method is more efficient than executing the query and browse the
   * returning Iterable.
   * 
   * @return
   */
  @Override
  public long count() {
    if (hasContainers.isEmpty()) {
      // NO CONDITIONS: USE THE FAST COUNT
      long counter = ((OrientVertex) vertex).countEdges(direction, labels);
      if (limit != Integer.MAX_VALUE && counter > limit)
        return limit;
      return counter;
    }

    // ITERATE EDGES TO MATCH CONDITIONS
    return super.count();
  }
}
