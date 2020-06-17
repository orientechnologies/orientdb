package com.orientechnologies.orient.object.db.entity;

import com.orientechnologies.orient.core.annotation.OId;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/** Created by tglman on 17/02/17. */
public class SimpleParent {

  private SimpleChild child;

  @OId private OIdentifiable id;

  public SimpleChild getChild() {
    return child;
  }

  public void setChild(SimpleChild child) {
    this.child = child;
  }

  public OIdentifiable getId() {
    return id;
  }

  public void setId(OIdentifiable id) {
    this.id = id;
  }
}
