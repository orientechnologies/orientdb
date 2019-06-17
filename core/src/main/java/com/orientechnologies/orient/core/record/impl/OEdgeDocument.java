package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;

public class OEdgeDocument extends ODocument implements OEdge {

  public OEdgeDocument(OClass cl) {
    super(cl);
  }

  public OEdgeDocument() {
    super();
  }

  public OEdgeDocument(ODatabaseSession session) {
    super(session);
  }

  @Override
  public OVertex getFrom() {
    Object result = getProperty(DIRECTION_OUT);
    if (!(result instanceof OElement)) {
      return null;
    }
    OElement v = (OElement) result;
    if (!v.isVertex()) {
      return null;
    }
    return v.asVertex().get();
  }

  @Override
  public OVertex getTo() {
    Object result = getProperty(DIRECTION_IN);
    if (!(result instanceof OElement)) {
      return null;
    }
    OElement v = (OElement) result;
    if (!v.isVertex()) {
      return null;
    }
    return v.asVertex().get();
  }

  @Override
  public boolean isLightweight() {
    //LIGHTWEIGHT EDGES MANAGED BY OEdgeDelegate, IN FUTURE MAY BE WE NEED TO HANDLE THEM WITH THIS
    return false;
  }

  public OEdgeDocument delete() {
    OEdgeDelegate.deleteLinks(this);
    super.delete();
    return this;
  }

}
