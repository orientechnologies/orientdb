package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.Message;
import com.orientechnologies.website.model.schema.dto.OUser;

import java.util.Date;

/**
 * Created by Enrico Risa on 19/01/15.
 */
public interface MessageRepository extends BaseRepository<Message> {

  public Message getLastMessage(Integer roomId);

  public Message findById(String id);

  public Date findLastActivity(OUser user, Client room);

  public Date findLastActivityIfNotNotified(OUser user, Client room);
}
