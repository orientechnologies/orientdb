package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OTopic;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Topic;
import com.orientechnologies.website.repository.TopicRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 28/05/15.
 */
@Repository
public class TopicRepositoryImpl extends OrientBaseRepository<Topic> implements TopicRepository {
  @Override
  public OTypeHolder<Topic> getHolder() {
    return OTopic.NUMBER;
  }

  @Override
  public Class<Topic> getEntityClass() {
    return Topic.class;
  }
}
