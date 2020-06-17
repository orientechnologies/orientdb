package com.orientechnologies.orient.object.db.entity;

import java.io.Serializable;

/** Created by tglman on 17/02/17. */
public class SimpleChild implements Serializable {
  private String name;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
