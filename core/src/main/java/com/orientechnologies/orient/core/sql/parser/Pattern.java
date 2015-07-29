package com.orientechnologies.orient.core.sql.parser;

import java.util.LinkedHashMap;
import java.util.Map;

/**
* Created by luigidellaquila on 28/07/15.
*/
class Pattern {
  Map<String, PatternNode> aliasToNode = new LinkedHashMap<String, PatternNode>();
  int                      numOfEdges  = 0;

  void addExpression(OMatchExpression expression) {
    PatternNode originNode = getOrCreateNode(expression.origin);

    for (OMatchPathItem item : expression.items) {
      String nextAlias = item.filter.getAlias();
      PatternNode nextNode = getOrCreateNode(item.filter);

      numOfEdges += originNode.addEdge(item, nextNode);
      originNode = nextNode;
    }
  }

  private PatternNode getOrCreateNode(OMatchFilter origin) {
    PatternNode originNode = get(origin.getAlias());
    if (originNode == null) {
      originNode = new PatternNode();
      originNode.alias = origin.getAlias();
      aliasToNode.put(originNode.alias, originNode);
    }
    return originNode;
  }

  PatternNode get(String alias) {
    return aliasToNode.get(alias);
  }

  int getNumOfEdges() {
    return numOfEdges;
  }
}
