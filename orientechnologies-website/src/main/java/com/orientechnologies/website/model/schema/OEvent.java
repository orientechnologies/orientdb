package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Event;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 06/11/14.
 */
public enum OEvent implements OTypeHolder<Event> {

  EVENT_ID("event_id") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  },
  CREATED_AT("createdAt") {
    @Override
    public OType getType() {
      return OType.DATETIME;
    }

  };
  private String name;

  OEvent(String name) {

    this.name = name;
  }

  @Override
  public Event fromDoc(ODocument doc, OrientBaseGraph graph) {

    if (Comment.class.getSimpleName().equals(doc.getClassName())) {
      return OComment.COMMENT_ID.fromDoc(doc, graph);
    }
    return null;
  }

  @Override
  public ODocument toDoc(Event doc, OrientBaseGraph graph) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return name;
  }
}
