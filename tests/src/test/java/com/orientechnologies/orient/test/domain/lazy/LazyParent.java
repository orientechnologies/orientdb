package com.orientechnologies.orient.test.domain.lazy;

import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToOne;

/** @author Wouter de Vaal */
public class LazyParent {

  @Id private String id;

  @OneToOne(fetch = FetchType.LAZY)
  private LazyChild child;

  @OneToOne(fetch = FetchType.LAZY)
  private LazyChild childCopy;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public LazyChild getChild() {
    return child;
  }

  public void setChild(LazyChild child) {
    this.child = child;
  }

  public LazyChild getChildCopy() {
    return childCopy;
  }

  public void setChildCopy(LazyChild childCopy) {
    this.childCopy = childCopy;
  }
}
