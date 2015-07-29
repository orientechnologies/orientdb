package com.orientechnologies.orient.core.sql.parser;

/**
 * Created by luigidellaquila on 28/07/15.
 */
class PatternEdge {
  PatternNode    in;
  PatternNode    out;
  OMatchPathItem item;

  Pattern        subPattern;
}
