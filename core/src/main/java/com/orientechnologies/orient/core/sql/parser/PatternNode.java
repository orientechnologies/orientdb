package com.orientechnologies.orient.core.sql.parser;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by luigidellaquila on 28/07/15.
 */
class PatternNode {
  String alias;
  Set<PatternEdge> out        = new LinkedHashSet<PatternEdge>();
  Set<PatternEdge> in         = new LinkedHashSet<PatternEdge>();
  int              centrality = 0;
  boolean          optional   = false;

  int addEdge(OMatchPathItem item, PatternNode to) {
    PatternEdge edge = new PatternEdge();
    edge.item = item;
    edge.out = this;
    edge.in = to;
    this.out.add(edge);
    to.in.add(edge);
    return 1;
  }

  public boolean isOptionalNode() {
    return optional;
  }
}
