package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.helper.OSequenceHelper;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/12/14.
 */
public enum OEnvironment implements OTypeHolder<Environment> {

  NAME("name") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  EID("eid") {
    @Override
    public OType getType() {
      return OType.LONG;
    }
  },
  OS("os") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  JVM("jvm") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  RAM("ram") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  CONNECTION_TYPE("connectionType") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  DESCRIPTION("description") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  VERSION("version") {
    @Override
    public OType getType() {
      return OType.LINK;
    }
  },
  DISTRIBUTED("distributed") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  NOTE("note") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  };
  private String name;

  OEnvironment(String name) {
    this.name = name;
  }

  @Override
  public Environment fromDoc(ODocument doc, OrientBaseGraph graph) {
    if (doc == null) {
      return null;
    }
    Environment env = new Environment();
    env.setId(doc.getIdentity().toString());
    env.setEid((Long) doc.field(EID.toString()));
    env.setName((String) doc.field(NAME.toString()));
    env.setDescription((String) doc.field(DESCRIPTION.toString()));
    env.setConnectionType((String) doc.field(CONNECTION_TYPE.toString()));
    env.setJvm((String) doc.field(JVM.toString()));
    env.setOs((String) doc.field(OS.toString()));
    env.setRam((String) doc.field(RAM.toString()));
    env.setDistributed((Boolean) doc.field(DISTRIBUTED.toString()));
    env.setNote((String) doc.field(NOTE.toString()));
    env.setVersion(OMilestone.NUMBER.fromDoc(doc.<ODocument> field(VERSION.toString()), graph));

    return env;
  }

  @Override
  public ODocument toDoc(Environment entity, OrientBaseGraph graph) {
    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
      long next = OSequenceHelper.next(graph, entity.getClass().getSimpleName());
      doc.field(EID.toString(), next);
      entity.setEid(next);
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(EID.toString(), entity.getEid());
    doc.field(NAME.toString(), entity.getName());
    doc.field(DESCRIPTION.toString(), entity.getDescription());
    doc.field(CONNECTION_TYPE.toString(), entity.getConnectionType());
    doc.field(JVM.toString(), entity.getJvm());
    doc.field(OS.toString(), entity.getOs());
    doc.field(RAM.toString(), entity.getRam());
    doc.field(DISTRIBUTED.toString(), entity.getDistributed());
    doc.field(NOTE.toString(), entity.getNote());
    doc.field(VERSION.toString(), OMilestone.NUMBER.toDoc(entity.getVersion(), graph));
    return doc;
  }

  @Override
  public String toString() {
    return name;
  }
}
