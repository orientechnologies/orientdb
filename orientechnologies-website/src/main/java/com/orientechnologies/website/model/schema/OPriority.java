package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Priority;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 24/11/14.
 */
public enum OPriority implements OTypeHolder<Priority> {
  NAME("name") {
    @Override
    public OType getType() {
      return OType.STRING;
    }

  },
  NUMBER("number") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }

  },
  COLOR("color") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  };
  private String name;

  OPriority(String name) {

    this.name = name;
  }

  @Override
  public Priority fromDoc(ODocument doc, OrientBaseGraph graph) {
    if (doc == null) {
      return null;
    }
    Priority l = new Priority();
    l.setId(doc.getIdentity().toString());
    l.setName((String) doc.field(NAME.toString()));
    l.setNumber((Integer) doc.field(NUMBER.toString()));
    l.setColor((String) doc.field(COLOR.toString()));

    return l;
  }

  @Override
  public ODocument toDoc(Priority entity, OrientBaseGraph graph) {
    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(NAME.toString(), entity.getName());
    doc.field(NUMBER.toString(), entity.getNumber());
    doc.field(COLOR.toString(), entity.getColor());
    return doc;
  }

  @Override
  public String toString() {
    return name;
  }
}
