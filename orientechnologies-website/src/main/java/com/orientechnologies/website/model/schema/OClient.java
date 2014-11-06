package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Client;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public enum OClient implements OTypeHolder<Client> {
  NAME("name") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  };

  private final String description;

  OClient(String description) {
    this.description = description;
  }

  @Override
  public ODocument toDoc(Client doc, OrientBaseGraph graph) {
    return null;
  }

  @Override
  public Client fromDoc(ODocument doc, OrientBaseGraph graph) {
    return null;
  }

  @Override
  public String toString() {
    return description;
  }
}
