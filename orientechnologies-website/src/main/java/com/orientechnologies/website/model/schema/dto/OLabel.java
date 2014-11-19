package com.orientechnologies.website.model.schema.dto;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 06/11/14.
 */
public enum OLabel implements OTypeHolder<Label> {
  NAME("name") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  COLOR("color") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  };
  private String name;

  OLabel(String name) {
    this.name = name;
  }

  @Override
  public Label fromDoc(ODocument doc, OrientBaseGraph graph) {
    if (doc == null) {
      return null;
    }
    Label l = new Label();
    l.setId(doc.getIdentity().toString());
    l.setName((String) doc.field(NAME.toString()));
    l.setColor((String) doc.field(COLOR.toString()));
    return l;
  }

  @Override
  public ODocument toDoc(Label entity, OrientBaseGraph graph) {
    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(NAME.toString(), entity.getName());
    doc.field(COLOR.toString(), entity.getColor());
    return doc;
  }

  @Override
  public String toString() {
    return name;
  }
}
