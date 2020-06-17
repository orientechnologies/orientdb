package com.orientechnologies.orient.test.domain.cycle;

import java.util.ArrayList;
import java.util.List;

/** @author Wouter de Vaal */
public class CycleParent {

  private String name;

  private List<CycleChild> children = new ArrayList<CycleChild>();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<CycleChild> getChildren() {
    return children;
  }

  public void setChildren(List<CycleChild> children) {
    this.children = children;
  }
}
