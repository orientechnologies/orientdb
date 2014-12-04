package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.orientechnologies.website.model.schema.dto.Sla;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.List;

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
  DESCRIPTION("description") {
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
    env.setName((String) doc.field(NAME.toString()));
    env.setDescription((String) doc.field(DESCRIPTION.toString()));
    OrientVertex iss = graph.getVertex(doc);
    List<Sla> slaList = new ArrayList<Sla>();
    for (Edge edge : iss.getEdges(Direction.OUT, HasSla.class.getSimpleName())) {
      OrientEdge e = (OrientEdge) edge;
      slaList.add(HasSla.RANGE.fromDoc(e.getRecord(), graph));
    }
    return env;
  }

  @Override
  public ODocument toDoc(Environment entity, OrientBaseGraph graph) {
    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(NAME.toString(), entity.getName());
    doc.field(DESCRIPTION.toString(), entity.getDescription());
    return doc;
  }

  @Override
  public String toString() {
    return name;
  }
}
