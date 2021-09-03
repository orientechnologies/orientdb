package com.orientechnologies.orient.core.sql.executor;

import java.util.ArrayList;
import java.util.List;

/** Created by luigidellaquila on 19/12/16. */
public class OInfoExecutionStep implements OExecutionStep {

  private String name;
  private String type;
  private String javaType;
  private String targetNode;
  private String description;
  private long cost;
  private List<OExecutionStep> subSteps = new ArrayList<>();

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public String getTargetNode() {
    return targetNode;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public List<OExecutionStep> getSubSteps() {
    return subSteps;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public OResult toResult() {
    return null;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setTargetNode(String targetNode) {
    this.targetNode = targetNode;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setCost(long cost) {
    this.cost = cost;
  }

  public String getJavaType() {
    return javaType;
  }

  public void setJavaType(String javaType) {
    this.javaType = javaType;
  }
}
