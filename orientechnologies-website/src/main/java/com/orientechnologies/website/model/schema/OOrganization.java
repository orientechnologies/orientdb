package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum OOrganization implements OTypeHolder<com.orientechnologies.website.model.schema.dto.Organization> {
  NAME("name") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  CLOSING_DAYS("closingDays") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  },
  CLOSING_MESSAGE("closingMessage") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  };

  private final String name;

  OOrganization(String name) {
    this.name = name;
  }

  @Override
  public ODocument toDoc(com.orientechnologies.website.model.schema.dto.Organization entity, OrientBaseGraph graph) {
    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(NAME.toString(), entity.getName());
    doc.field(CLOSING_DAYS.toString(), entity.getClosingDays());
    doc.field(CLOSING_MESSAGE.toString(), entity.getClosingMessage());
    return doc;
  }

  @Override
  public com.orientechnologies.website.model.schema.dto.Organization fromDoc(ODocument doc, OrientBaseGraph graph) {
    com.orientechnologies.website.model.schema.dto.Organization organization = new com.orientechnologies.website.model.schema.dto.Organization();
    organization.setId(doc.getIdentity().toString());
    organization.setName((String) doc.field(OOrganization.NAME.toString()));
    organization.setClosingDays((Integer) doc.field(CLOSING_DAYS.toString()));
    organization.setClosingMessage((String) doc.field(CLOSING_MESSAGE.toString()));
    return organization;
  }

  @Override
  public String toString() {
    return name;
  }
}
