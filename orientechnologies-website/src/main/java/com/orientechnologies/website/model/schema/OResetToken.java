package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.ResetToken;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import java.util.Calendar;
import java.util.UUID;

/**
 * Created by Enrico Risa on 05/07/2018.
 */
public enum OResetToken implements OTypeHolder<ResetToken> {

  TOKEN("token") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  }, EXPIRE("expire") {
    @Override
    public OType getType() {
      return OType.DATETIME;
    }
  }, USER("user") {
    @Override
    public OType getType() {
      return OType.LINK;
    }
  };

  private final String description;

  OResetToken(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }

  @Override
  public ResetToken fromDoc(ODocument doc, OrientBaseGraph graph) {

    ResetToken token = new ResetToken();
    token.setId(doc.getIdentity().toString());
    token.setExpire(doc.field(EXPIRE.toString()));
    token.setToken(doc.field(TOKEN.toString()));

    OIdentifiable user = doc.field(USER.toString());
    com.orientechnologies.website.model.schema.dto.OUser oUser = OUser.NAME.fromDoc(graph.getRawGraph().load(user.getIdentity()), graph);
    token.setUser(oUser);

    return token;
  }

  @Override
  public ODocument toDoc(ResetToken entity, OrientBaseGraph graph) {

    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
      doc.field(TOKEN.toString(), UUID.randomUUID().toString());

      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.MINUTE, 60 * 24);
      doc.field(EXPIRE.toString(), cal.getTime());
      doc.field(USER.toString(), new ORecordId(entity.getUser().getRid()));
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    } return doc;
  }
}
