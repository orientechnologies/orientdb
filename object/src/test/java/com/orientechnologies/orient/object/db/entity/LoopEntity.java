package com.orientechnologies.orient.object.db.entity;

/** Created by tglman on 09/05/16. */
public class LoopEntity {

  private LoopEntityLink link;
  private String name;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public LoopEntityLink getLink() {
    return link;
  }

  public void setLink(LoopEntityLink link) {
    this.link = link;
  }
}
