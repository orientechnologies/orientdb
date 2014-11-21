package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Event;
import com.orientechnologies.website.model.schema.dto.IssueEvent;
import com.orientechnologies.website.model.schema.dto.IssueEventInternal;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 06/11/14.
 */
public enum OEvent implements OTypeHolder<Event> {

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
    } else if (IssueEvent.class.getSimpleName().equals(doc.getClassName())) {
      return OIssueEvent.EVENT_ID.fromDoc(doc, graph);
    } else if (IssueEventInternal.class.getSimpleName().equals(doc.getClassName())) {
      return OIssueEventInternal.VERSION.fromDoc(doc, graph);
    }
    return null;
  }

  @Override
  public ODocument toDoc(Event event, OrientBaseGraph graph) {

    ODocument doc = null;
    if (event instanceof IssueEventInternal) {
      doc = OIssueEventInternal.VERSION.toDoc((IssueEventInternal) event, graph);
    } else if (event instanceof IssueEvent) {
      doc = OIssueEvent.EVENT_ID.toDoc((IssueEvent) event, graph);
    } else if (event instanceof Comment) {
      doc = OComment.COMMENT_ID.toDoc((Comment) event, graph);
    }
    return doc;
  }

  @Override
  public String toString() {
    return name;
  }
}
