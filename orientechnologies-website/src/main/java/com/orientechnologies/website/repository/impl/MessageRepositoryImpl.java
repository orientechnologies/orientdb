package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.model.schema.OMessage;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.Message;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.MessageRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 19/01/15.
 */
@Repository
public class MessageRepositoryImpl extends OrientBaseRepository<Message> implements MessageRepository {
  @Override
  public OTypeHolder<Message> getHolder() {
    return OMessage.UUID;
  }

  @Override
  public Class<Message> getEntityClass() {
    return Message.class;
  }

  @Override
  public Message getLastMessage(Integer roomId) {
    OrientGraph db = dbFactory.getGraph();

    OClass oClass = db.getRawGraph().getMetadata().getSchema().getClass("Chat" + roomId);
    if (oClass == null) {
      return null;
    }
    String query = String.format("select * from %s order by @rid desc limit 1", "Chat" + roomId);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();
      return OMessage.UUID.fromDoc(doc, db);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public Message findById(String id) {

    OrientGraph db = dbFactory.getGraph();

    ODocument msg = db.getRawGraph().load(new ORecordId(id));
    return OMessage.UUID.fromDoc(msg, db);
  }

  @Override
  public Date findLastActivity(final OUser user, final Client room) {

    ODocument activity = findActivityForUserAndRoom(user, room);
    return activity != null ? (Date) activity.field("timestamp") : null;
  }

  private ODocument findActivityForUserAndRoom(final OUser user, final Client room) {
    OrientGraph db = dbFactory.getGraph();
    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("user", user.getRid());
        put("room", room.getId());

      }
    };
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL("select from ChatLog WHERE user=:user and room =:room")).execute(
        params);

    try {
      ODocument doc = vertices.iterator().next().getRecord();
      doc = doc.reload(null, true);
      return doc;
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public Date findLastActivityIfNotNotified(OUser user, Client room) {
    ODocument activity = findActivityForUserAndRoom(user, room);
    return activity != null ? (!Boolean.TRUE.equals(activity.field("notified")) ? (Date) activity.field("timestamp") : null) : null;
  }
}
