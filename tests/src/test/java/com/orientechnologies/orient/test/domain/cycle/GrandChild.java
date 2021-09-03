package com.orientechnologies.orient.test.domain.cycle;

/** @author Wouter de Vaal */
public class GrandChild {

  private String name;

  private CycleParent grandParent;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public CycleParent getGrandParent() {
    return grandParent;
  }

  public void setGrandParent(CycleParent grandParent) {
    this.grandParent = grandParent;
  }
}
