package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

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
    if (origin.isOptional()) {
      originNode.optional = true;
    }
    return originNode;
  }

  PatternNode get(String alias) {
    return aliasToNode.get(alias);
  }

  int getNumOfEdges() {
    return numOfEdges;
  }

  public void validate() {
    for (PatternNode node : this.aliasToNode.values()) {
      if (node.isOptionalNode()) {
        if (node.out.size() > 0) {
          throw new OCommandSQLParsingException(
              "In current MATCH version, optional nodes are allowed only on right terminal nodes, eg. {} --> {optional:true} is allowed, {optional:true} <-- {} is not. ");
        }
        if(node.in.size()==0){
          throw new OCommandSQLParsingException(
              "In current MATCH version, optional nodes must have at least one incoming pattern edge");
        }
//        if (node.in.size() != 1) {
//          throw new OCommandSQLParsingException("In current MATCH version, optional nodes are allowed only as single terminal nodes. ");
//        }
      }
    }
  }
}
