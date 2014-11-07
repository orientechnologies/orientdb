package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OEvent;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Event;
import com.orientechnologies.website.repository.EventRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 06/11/14.
 */
@Repository
public class EventRepositoryImpl extends OrientBaseRepository<Event> implements EventRepository {
  @Override
  public OTypeHolder<Event> getHolder() {
    return OEvent.CREATED_AT;
  }

  @Override
  public Class<Event> getEntityClass() {
    return Event.class;
  }
}
