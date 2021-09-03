package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.PatternEdge;
import com.orientechnologies.orient.core.sql.executor.PatternNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 28/07/15. */
public class Pattern {
  public Map<String, PatternNode> aliasToNode = new LinkedHashMap<String, PatternNode>();
  public int numOfEdges = 0;

  public void addExpression(OMatchExpression expression) {
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

  public PatternNode get(String alias) {
    return aliasToNode.get(alias);
  }

  public int getNumOfEdges() {
    return numOfEdges;
  }

  public void validate() {
    for (PatternNode node : this.aliasToNode.values()) {
      if (node.isOptionalNode()) {
        if (node.out.size() > 0) {
          throw new OCommandSQLParsingException(
              "In current MATCH version, optional nodes are allowed only on right terminal nodes, eg. {} --> {optional:true} is allowed, {optional:true} <-- {} is not. ");
        }
        if (node.in.size() == 0) {
          throw new OCommandSQLParsingException(
              "In current MATCH version, optional nodes must have at least one incoming pattern edge");
        }
        //        if (node.in.size() != 1) {
        //          throw new OCommandSQLParsingException("In current MATCH version, optional nodes
        // are allowed only as single terminal nodes. ");
        //        }
      }
    }
  }

  /**
   * splits this pattern into multiple
   *
   * @return
   */
  public List<Pattern> getDisjointPatterns() {
    Map<PatternNode, String> reverseMap = new IdentityHashMap<>();
    reverseMap.putAll(
        this.aliasToNode.entrySet().stream()
            .collect(Collectors.toMap(x -> x.getValue(), x -> x.getKey())));

    List<Pattern> result = new ArrayList<>();
    while (!reverseMap.isEmpty()) {
      Pattern pattern = new Pattern();
      result.add(pattern);
      Map.Entry<PatternNode, String> nextNode = reverseMap.entrySet().iterator().next();
      Set<PatternNode> toVisit = new HashSet<>();
      toVisit.add(nextNode.getKey());
      while (toVisit.size() > 0) {
        PatternNode currentNode = toVisit.iterator().next();
        toVisit.remove(currentNode);
        if (reverseMap.containsKey(currentNode)) {
          pattern.aliasToNode.put(reverseMap.get(currentNode), currentNode);
          reverseMap.remove(currentNode);
          for (PatternEdge x : currentNode.out) {
            toVisit.add(x.in);
          }
          for (PatternEdge x : currentNode.in) {
            toVisit.add(x.out);
          }
        }
      }
      pattern.recalculateNumOfEdges();
    }
    return result;
  }

  private void recalculateNumOfEdges() {
    Map<PatternEdge, PatternEdge> edges = new IdentityHashMap<>();
    for (PatternNode node : this.aliasToNode.values()) {
      for (PatternEdge edge : node.out) {
        edges.put(edge, edge);
      }
      for (PatternEdge edge : node.in) {
        edges.put(edge, edge);
      }
    }
    this.numOfEdges = edges.size();
  }

  public Map<String, PatternNode> getAliasToNode() {
    return aliasToNode;
  }

  public void setAliasToNode(Map<String, PatternNode> aliasToNode) {
    this.aliasToNode = aliasToNode;
  }

  public void setNumOfEdges(int numOfEdges) {
    this.numOfEdges = numOfEdges;
  }
}
