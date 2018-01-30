package com.orientechnologies.orient.core.record.impl;

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

  @Override
  public OVertex getFrom() {
    Object result = getProperty(DIRECITON_OUT);
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
    Object result = getProperty(DIRECITON_IN);
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
    //TODO:FIND A WAY FOR LIGTHWEIGHT
    return false;
  }
}
