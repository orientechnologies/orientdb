package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OMessage;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Message;
import com.orientechnologies.website.repository.MessageRepository;
import org.springframework.stereotype.Repository;

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
}
