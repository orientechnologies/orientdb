package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OTopicComment;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.TopicComment;
import com.orientechnologies.website.repository.TopicCommentRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 29/05/15.
 */
@Repository
public class TopicCommentRepositoryImpl extends OrientBaseRepository<TopicComment> implements TopicCommentRepository {
  @Override
  public OTypeHolder<TopicComment> getHolder() {
    return OTopicComment.UUID;
  }

  @Override
  public Class<TopicComment> getEntityClass() {
    return TopicComment.class;
  }
}
