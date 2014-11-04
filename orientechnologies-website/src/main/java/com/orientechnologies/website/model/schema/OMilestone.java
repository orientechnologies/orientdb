package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Milestone;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum OMilestone implements OTypeHolder<Milestone> {
  NUMBER("number") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  },
  TITLE("title") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  },
  CREATOR("creator") {
    @Override
    public OType getType() {
      return OType.LINK;
    }
  },
  DESCRIPTION("description") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  },
  STATE("state") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  };

  private String name;

  OMilestone(String name) {
    this.name = name;
  }

  @Override
  public Milestone fromDoc(ODocument doc, OrientBaseGraph graph) {
    return null;
  }

  @Override
  public ODocument toDoc(Milestone doc, OrientBaseGraph graph) {
    return null;
  }
}
