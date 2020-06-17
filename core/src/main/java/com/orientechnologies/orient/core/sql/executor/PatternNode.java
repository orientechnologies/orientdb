package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import java.util.LinkedHashSet;
import java.util.Set;

/** Created by luigidellaquila on 28/07/15. */
public class PatternNode {
  public String alias;
  public Set<PatternEdge> out = new LinkedHashSet<PatternEdge>();
  public Set<PatternEdge> in = new LinkedHashSet<PatternEdge>();
  public int centrality = 0;
  public boolean optional = false;

  public int addEdge(OMatchPathItem item, PatternNode to) {
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
