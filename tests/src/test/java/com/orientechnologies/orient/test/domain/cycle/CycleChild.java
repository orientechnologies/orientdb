package com.orientechnologies.orient.test.domain.cycle;

import java.util.HashSet;
import java.util.Set;

/** @author Wouter de Vaal */
public class CycleChild {

  private String name;

  private CycleParent parent;

  private Set<GrandChild> grandChildren = new HashSet<GrandChild>();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public CycleParent getParent() {
    return parent;
  }

  public void setParent(CycleParent parent) {
    this.parent = parent;
  }

  public Set<GrandChild> getGrandChildren() {
    return grandChildren;
  }

  public void setGrandChildren(Set<GrandChild> grandChildren) {
    this.grandChildren = grandChildren;
  }
}
