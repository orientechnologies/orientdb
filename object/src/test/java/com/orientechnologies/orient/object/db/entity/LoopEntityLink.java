package com.orientechnologies.orient.object.db.entity;

/** Created by tglman on 09/05/16. */
public class LoopEntityLink {
  private LoopEntity linkBack;

  public LoopEntity getLinkBack() {
    return linkBack;
  }

  public void setLinkBack(LoopEntity linkBack) {
    this.linkBack = linkBack;
  }
}
