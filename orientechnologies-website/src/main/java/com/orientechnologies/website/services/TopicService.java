package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.Topic;
import com.orientechnologies.website.model.schema.dto.TopicComment;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by Enrico Risa on 29/05/15.
 */
public interface TopicService {

  @Transactional
  public void deleteSingleTopic(Topic topic);

  @Transactional
  public TopicComment postComment(Topic topic, TopicComment comment);

  @Transactional
  public TopicComment patchComment(TopicComment comment, TopicComment patch);

  @Transactional
  public Topic patchTopic(Topic singleTopicByNumber, Topic patch);

  @Transactional
  public void deleteSingleTopicComment(TopicComment comment);
}
