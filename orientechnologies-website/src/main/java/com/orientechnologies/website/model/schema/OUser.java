package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/11/14.
 */

public enum OUser implements OTypeHolder<com.orientechnologies.website.model.schema.dto.OUser> {
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
  },
  CONFIRMED("confirmed") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  NOTIFICATION("notification") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  WATCHING("watching") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  PUBLIC_MUTE("publicMute") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  INVITED("invited") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  CHAT_NOTIFICATION("chatNotification") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  FIRSTNAME("firstName") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  SECONDNAME("secondName") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  WORKINGEMAIL("workingEmail") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  COMPANY("company") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  };

  @Override
  public com.orientechnologies.website.model.schema.dto.OUser fromDoc(ODocument doc, OrientBaseGraph graph) {
    if (doc == null) {
      return null;
    }
    com.orientechnologies.website.model.schema.dto.OUser user = new com.orientechnologies.website.model.schema.dto.OUser();
    user.setEmail((String) doc.field(EMAIL.toString()));
    user.setRid(doc.getIdentity().toString());
    user.setId((Long) doc.field(ID.toString()));
    user.setName((String) doc.field(NAME.toString()));
    user.setToken((String) doc.field(TOKEN.toString()));
    Boolean confirmed = doc.field(CONFIRMED.toString());
    user.setFirstName((String) doc.field(FIRSTNAME.toString()));
    user.setNotification((Boolean) doc.field(NOTIFICATION.toString()));
    user.setChatNotification((Boolean) doc.field(CHAT_NOTIFICATION.toString()));
    user.setWatching((Boolean) doc.field(WATCHING.toString()));
    user.setInvited((Boolean) doc.field(INVITED.toString()));
    user.setSecondName((String) doc.field(SECONDNAME.toString()));
    user.setCompany((String) doc.field(COMPANY.toString()));
    user.setWorkingEmail((String) doc.field(WORKINGEMAIL.toString()));
    user.setPublicMute((Boolean) doc.field(PUBLIC_MUTE.toString()));
    user.setConfirmed(confirmed != null ? confirmed : false);

    return user;
  }

  @Override
  public ODocument toDoc(com.orientechnologies.website.model.schema.dto.OUser entity, OrientBaseGraph graph) {
    ODocument doc = null;
    if (entity.getRid() == null) {
      doc = new ODocument(OUser.class.getSimpleName());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getRid()));
    }
    doc.field(NAME.toString(), entity.getName());
    doc.field(ID.toString(), entity.getId());
    if (entity.getToken() != null) {
      doc.field(TOKEN.toString(), entity.getToken());
    }
    if (entity.getEmail() != null) {
      doc.field(EMAIL.toString(), entity.getEmail());
    }
    doc.field(CONFIRMED.toString(), entity.getConfirmed());
    doc.field(COMPANY.toString(), entity.getCompany());
    doc.field(FIRSTNAME.toString(), entity.getFirstName());
    doc.field(SECONDNAME.toString(), entity.getSecondName());
    doc.field(WORKINGEMAIL.toString(), entity.getWorkingEmail());
    doc.field(NOTIFICATION.toString(), entity.getNotification());
    doc.field(CHAT_NOTIFICATION.toString(), entity.getChatNotification());
    doc.field(WATCHING.toString(), entity.getWatching());
    doc.field(PUBLIC_MUTE.toString(), entity.getPublicMute());
    doc.field(INVITED.toString(), entity.getInvited());
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
