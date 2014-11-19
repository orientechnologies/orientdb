package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.User;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/11/14.
 */

public enum OUser implements OTypeHolder<User> {
  ID("id") {
    @Override
    public OType getType() {
      return OType.LONG;
    }
  },
  NAME("name") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },

  TOKEN("token") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  EMAIL("email") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  };

  @Override
  public User fromDoc(ODocument doc, OrientBaseGraph graph) {
    if (doc == null) {
      return null;
    }
    User user = new User();
    user.setEmail((String) doc.field(EMAIL.toString()));
    user.setRid(doc.getIdentity().toString());
    user.setId((Long) doc.field(ID.toString()));
    user.setName((String) doc.field(NAME.toString()));
    user.setToken((String) doc.field(TOKEN.toString()));
    return user;
  }

  @Override
  public ODocument toDoc(User entity, OrientBaseGraph graph) {
    ODocument doc = null;
    if (entity.getRid() == null) {
      doc = new ODocument(OUser.class.getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getRid()));
    }
    doc.field(NAME.toString(), entity.getName());
    doc.field(ID.toString(), entity.getId());
    doc.field(TOKEN.toString(), entity.getToken());
    doc.field(EMAIL.toString(), entity.getEmail());
    doc.field("status", "active");
    doc.field("password", "test");
    return doc;
  }

  private final String description;

  OUser(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
