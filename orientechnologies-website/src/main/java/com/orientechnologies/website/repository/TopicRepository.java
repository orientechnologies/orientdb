package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Topic;
import com.orientechnologies.website.model.schema.dto.TopicComment;

import java.util.List;

/**
 * Created by Enrico Risa on 28/05/15.
 */
public interface TopicRepository extends BaseRepository<Topic> {
  TopicComment findTopicCommentByUUID(String name, Long number, String uuid);

  List<TopicComment> findTopicComments(String name,Long number);
}
