package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Created by Enrico Risa on 04/11/14.
 */

public enum ORepository implements OTypeHolder<com.orientechnologies.website.model.schema.dto.Repository> {
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
  private final String description;

  ORepository(String description) {
    this.description = description;
  }

  @Override
  public ODocument toDoc(com.orientechnologies.website.model.schema.dto.Repository entity, OrientBaseGraph graph) {
    ODocument doc = null;
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
  public com.orientechnologies.website.model.schema.dto.Repository fromDoc(ODocument doc, OrientBaseGraph graph) {
    com.orientechnologies.website.model.schema.dto.Repository repo = new com.orientechnologies.website.model.schema.dto.Repository();
    repo.setName((String) doc.field(NAME.toString()));
    repo.setDescription((String) doc.field(DESCRIPTION.toString()));
    repo.setId(doc.getIdentity().toString());

    OrientVertex iss = graph.getVertex(doc);
    for (Vertex vertex : iss.getVertices(Direction.IN, HasRepo.class.getSimpleName())) {
      repo.setOrganization(OOrganization.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }
    return repo;
  }

  @Override
  public String toString() {
    return description;
  }
}