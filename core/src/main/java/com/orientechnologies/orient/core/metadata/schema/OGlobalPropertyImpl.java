package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;

public class OGlobalPropertyImpl implements OGlobalProperty, ODocumentSerializable {

  private String  name;
  private OType   type;
  private Integer id;

  public OGlobalPropertyImpl() {
  }

  public OGlobalPropertyImpl(String name, OType type, Integer id) {
    this.name = name;
    this.type = type;
    this.id = id;
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public OType getType() {
    return type;
  }

  @Override
  public void fromDocument(ODocument document) {
    this.name = document.field("name");
    this.type = OType.valueOf((String) document.field("type"));
    this.id = document.field("id");
  }

  @Override
  public ODocument toDocument() {
    ODocument doc = new ODocument();
    doc.field("name", name);
    doc.field("type", type.name());
    doc.field("id", id);
    return doc;
  }
}
