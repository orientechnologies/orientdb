package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Client;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum OClient implements OTypeHolder<Client> {
  NAME("name") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  CLIENT_ID("clientId") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  },
  SUPPORT("support") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  };
  private final String description;

  OClient(String description) {
    this.description = description;
  }

  @Override
  public ODocument toDoc(Client entity, OrientBaseGraph graph) {

    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(NAME.toString(), entity.getName());
    doc.field(CLIENT_ID.toString(), entity.getClientId());
    doc.field(SUPPORT.toString(), entity.isSupport());
    return doc;
  }

  @Override
  public Client fromDoc(ODocument doc, OrientBaseGraph graph) {

    if (doc == null) {
      return null;
    }
    Client l = new Client();
    l.setId(doc.getIdentity().toString());
    l.setName((String) doc.field(NAME.toString()));
    l.setClientId((Integer) doc.field(CLIENT_ID.toString()));
    Boolean field = (Boolean) doc.field(SUPPORT.toString());
    l.setSupport(field != null ? field : false);

    return l;
  }

  @Override
  public String toString() {
    return description;
  }
}
