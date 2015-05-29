package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.model.schema.OTopic;
import com.orientechnologies.website.model.schema.OTopicComment;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Topic;
import com.orientechnologies.website.model.schema.dto.TopicComment;
import com.orientechnologies.website.repository.TopicRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

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

  @Override
  public TopicComment findTopicCommentByUUID(String name, Long number, String uuid) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format(
        "select expand(out('HasTopic')[number = %d].out('HasComment')[uuid = '%s']) from Organization where name = '%s') ", number,
        uuid, name);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();
      return OTopicComment.UUID.fromDoc(doc, db);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public List<TopicComment> findTopicComments(String name, Long number) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format(
        "select expand(out('HasTopic')[number = %d].out('HasComment')) from Organization where name = '%s') ", number, name);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    List<TopicComment> comments = new ArrayList<TopicComment>();
    for (OrientVertex vertice : vertices) {
      comments.add(OTopicComment.UUID.fromDoc(vertice.getRecord(), db));
    }
    return comments;
  }
}
