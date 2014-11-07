package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.IssueEvent;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import java.util.Date;

/**
 * Created by Enrico Risa on 07/11/14.
 */
public enum OIssueEvent implements OTypeHolder<IssueEvent> {
  EVENT_ID("event_id") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }

  },
  EVENT("event") {
    @Override
    public OType getType() {
      return OType.STRING;
    }

  },
  ACTOR("actor") {
    @Override
    public OType getType() {
      return OType.LINK;
    }

  };
  private String name;

  OIssueEvent(String name) {

    this.name = name;
  }

  @Override
  public IssueEvent fromDoc(ODocument doc, OrientBaseGraph graph) {
    IssueEvent event = new IssueEvent();
    event.setId(doc.getIdentity().toString());
    event.setEventId((Integer) doc.field(EVENT_ID.toString()));
    event.setEvent((String) doc.field(EVENT.toString()));
    event.setCreatedAt((Date) doc.field(OEvent.CREATED_AT.toString()));
    event.setActor(OUser.NAME.fromDoc((ODocument) doc.field(ACTOR.toString()), graph));
    return event;
  }

  @Override
  public ODocument toDoc(IssueEvent entity, OrientBaseGraph graph) {

    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(EVENT.toString(), entity.getEvent());
    doc.field(EVENT_ID.toString(), entity.getEventId());
    doc.field(OEvent.CREATED_AT.toString(), entity.getCreatedAt());
    doc.field(ACTOR.toString(), (entity.getActor() != null ? new ORecordId(entity.getActor().getId()) : null));
    return doc;
  }

  @Override
  public String toString() {
    return name;
  }
}
