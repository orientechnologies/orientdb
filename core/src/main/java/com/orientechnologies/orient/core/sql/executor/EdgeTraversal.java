package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.OWhereClause;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class EdgeTraversal {
  boolean out = true;
  public  PatternEdge  edge;
  private String       leftClass;
  private OWhereClause leftFilter;

  public EdgeTraversal(PatternEdge edge, boolean out) {
    this.edge = edge;
    this.out = out;
  }

  public void setLeftClass(String leftClass) {
    this.leftClass = leftClass;
  }

  public void setLeftFilter(OWhereClause leftFilter) {
    this.leftFilter = leftFilter;
  }

  public String getLeftClass() {
    return leftClass;
  }

  public OWhereClause getLeftFilter() {
    return leftFilter;
  }

  @Override
  public String toString() {
    return edge.toString();
  }
}