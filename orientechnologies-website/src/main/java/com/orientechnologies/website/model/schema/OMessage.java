package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Message;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import java.util.Date;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum OMessage implements OTypeHolder<Message> {
  UUID("uuid") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  BODY("body") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  DATE("date") {
    @Override
    public OType getType() {
      return OType.DATE;
    }
  },
  SENDER("sender") {
    @Override
    public OType getType() {
      return OType.LINK;
    }
  };
  private final String description;

  OMessage(String description) {
    this.description = description;
  }

  @Override
  public ODocument toDoc(Message entity, OrientBaseGraph graph) {

    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument("Chat" + entity.getClientId());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(UUID.toString(), entity.getUuid());
    doc.field(BODY.toString(), entity.getBody());
    doc.field(DATE.toString(), entity.getDate());
    doc.field(SENDER.toString(), (entity.getSender() != null ? new ORecordId(entity.getSender().getRid()) : null));
    return doc;
  }

  @Override
  public Message fromDoc(ODocument doc, OrientBaseGraph graph) {

    if (doc == null) {
      return null;
    }
    Message l = new Message();
    l.setId(doc.getIdentity().toString());
    l.setBody((String) doc.field(BODY.toString()));
    l.setDate((Date) doc.field(DATE.toString()));
    l.setSender(OUser.ID.fromDoc(doc.<ODocument> field(SENDER.toString()), graph));
    return l;
  }

  @Override
  public String toString() {
    return description;
  }
}
