package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Sla;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum HasSla implements OTypeHolder<Sla> {

  RANGE("range") {
    @Override
    public OType getType() {
      return OType.EMBEDDEDMAP;
    }
  },
  RESPONSE_TIME("responseTime") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  },
  RESOLVE_TIME("resolveTime") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  };

  private final String name;

  HasSla(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public Sla fromDoc(ODocument doc, OrientBaseGraph graph) {
    if (doc == null) {
      return null;
    }
    Sla env = new Sla();
    env.setId(doc.getIdentity().toString());
    env.setResponseTime((Integer) doc.field(RESPONSE_TIME.toString()));
    env.setRange((java.util.Map) doc.field(RANGE.toString()));

    return env;
  }

  @Override
  public ODocument toDoc(Sla entity, OrientBaseGraph graph) {
    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(RESPONSE_TIME.toString(), entity.getResponseTime());
    doc.field(RESOLVE_TIME.toString(), entity.getResolveTime());
    doc.field(RANGE.toString(), entity.getRange());
    return doc;
  }
}
