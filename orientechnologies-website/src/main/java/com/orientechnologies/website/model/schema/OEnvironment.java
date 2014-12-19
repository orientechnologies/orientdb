package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.helper.OSequenceHelper;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

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
  DISTRIBUTED("distributed") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
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
    env.setEid((Integer) doc.field(EID.toString()));
    env.setName((String) doc.field(NAME.toString()));
    env.setDescription((String) doc.field(DESCRIPTION.toString()));
    env.setConnectionType((String) doc.field(CONNECTION_TYPE.toString()));
    env.setJvm((String) doc.field(JVM.toString()));
    env.setOs((String) doc.field(OS.toString()));
    env.setRam((Integer) doc.field(RAM.toString()));
    env.setDistributed((Boolean) doc.field(DISTRIBUTED.toString()));
    OrientVertex iss = graph.getVertex(doc);

    for (Vertex vertex : iss.getVertices(Direction.OUT, HasVersion.class.getSimpleName())) {

      OrientVertex v = (OrientVertex) vertex;
      env.setVersion(OMilestone.NUMBER.fromDoc(v.getRecord(), graph));

    }
    return env;
  }

  @Override
  public ODocument toDoc(Environment entity, OrientBaseGraph graph) {
    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
      doc.field(EID.toString(), OSequenceHelper.next(graph, entity.getClass().getSimpleName()));
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
    doc.field(DISTRIBUTED.toString(), entity.getDescription());
    return doc;
  }

  @Override
  public String toString() {
    return name;
  }
}
