package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Optional;

public class OEdgeDocument extends ODocument implements OEdge {

  public OEdgeDocument(OClass cl) {
    super(cl);
  }

  public OEdgeDocument(ODatabaseSession session, String cl) {
    super(session, cl);
  }

  public OEdgeDocument() {
    super();
  }

  public OEdgeDocument(ODatabaseSession session) {
    super(session);
  }

  @Override
  public OVertex getFrom() {
    final Object result = getProperty(DIRECTION_OUT);
    return convertToVertex(result);
  }

  @Override
  public OVertex getTo() {
    final Object result = getProperty(DIRECTION_IN);
    return convertToVertex(result);
  }

  private static OVertex convertToVertex(final Object result) {
    if (!(result instanceof OIdentifiable)) {
      return null;
    }

    final OIdentifiable identifiable = (OIdentifiable) result;
    final OElement element;
    if (identifiable instanceof OElement) {
      element = (OElement) identifiable;
    } else {
      element = identifiable.getRecord();
    }

    return Optional.ofNullable(element).flatMap(OElement::asVertex).orElse(null);
  }

  @Override
  public boolean isLightweight() {
    // LIGHTWEIGHT EDGES MANAGED BY OEdgeDelegate, IN FUTURE MAY BE WE NEED TO HANDLE THEM WITH THIS
    return false;
  }

  public OEdgeDocument delete() {
    OEdgeDelegate.deleteLinks(this);
    super.delete();
    return this;
  }

  @Override
  public OEdgeDocument copy() {
    return (OEdgeDocument) super.copyTo(new OEdgeDocument());
  }
}
