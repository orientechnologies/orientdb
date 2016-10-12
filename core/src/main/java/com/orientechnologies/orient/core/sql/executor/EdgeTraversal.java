package com.orientechnologies.orient.core.sql.executor;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class EdgeTraversal {
  boolean out = true;
  public PatternEdge edge;

  public EdgeTraversal(PatternEdge edge, boolean out) {
    this.edge = edge;
    this.out = out;
  }
}