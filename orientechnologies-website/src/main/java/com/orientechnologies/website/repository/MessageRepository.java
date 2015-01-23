package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Message;

/**
 * Created by Enrico Risa on 19/01/15.
 */
public interface MessageRepository extends BaseRepository<Message> {

  public Message getLastMessage(Integer roomId);
}
