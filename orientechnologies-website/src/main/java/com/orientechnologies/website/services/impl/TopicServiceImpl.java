package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Tag;
import com.orientechnologies.website.model.schema.dto.Topic;
import com.orientechnologies.website.model.schema.dto.TopicComment;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.TopicCommentRepository;
import com.orientechnologies.website.repository.TopicRepository;
import com.orientechnologies.website.services.TopicService;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 29/05/15.
 */
@Service
public class TopicServiceImpl implements TopicService {

  @Autowired
  private OrientDBFactory        dbFactory;
  @Autowired
  private TopicRepository        topicRepository;

  @Autowired
  private TopicCommentRepository topicCommentRepository;

  @Autowired
  private SchemaManager          schemaManager;

  @Autowired
  private OrganizationRepository organizationRepository;

  @Transactional
  @Override
  public void deleteSingleTopic(Topic topic) {

    topicRepository.delete(topic);
  }

  @Override
  public TopicComment postComment(Topic topic, TopicComment comment) {

    comment.setCreatedAt(new Date());
    OUser user = SecurityHelper.currentUser();
    comment.setUpdatedAt(new Date());
    comment.setUser(user);
    comment = topicCommentRepository.save(comment);
    createTopicCommentRelationship(topic, comment);
    return comment;
  }

  private void createTopicCommentRelationship(Topic topic, TopicComment comment) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(topic.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(comment.getId()));
    orgVertex.addEdge(HasComment.class.getSimpleName(), devVertex);
  }

  @Override
  public TopicComment patchComment(TopicComment comment, TopicComment patch) {

    if (patch.getBody() != null) {
      comment.setBody(patch.getBody());
    }
    comment = topicCommentRepository.save(comment);
    return comment;
  }

  @Transactional
  @Override
  public Topic patchTopic(Topic topic, Topic patch) {

    if (patch.getTitle() != null) {
      topic.setTitle(patch.getTitle());
    }
    if (patch.getBody() != null) {
      topic.setBody(patch.getBody());
    }
    topic = topicRepository.save(topic);
    return topic;
  }

  @Override
  public void deleteSingleTopicComment(TopicComment comment) {
    topicCommentRepository.delete(comment);
  }

  @Transactional
  @Override
  public void deleteSingleTopicTag(Topic singleTopicByNumber, String uuid) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(singleTopicByNumber.getId()));
    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasTag.class.getSimpleName())) {
      Vertex v = edge.getVertex(Direction.IN);
      if (uuid.equals(v.getProperty(OTopic.UUID.toString()))) {
        edge.remove();
      }
    }

  }

  @Override
  public void tagsTopic(Topic singleTopicByNumber, List<Tag> tags) {

    if (tags != null) {
      for (Tag tag : tags) {
        Tag tagByUUID = organizationRepository.findTagByUUID(singleTopicByNumber.getOrganization().getName(), tag.getUuid());
        if (tagByUUID != null) {
          schemaManager.connect(singleTopicByNumber, tagByUUID, HasTag.class.getSimpleName());
        }
      }
    }
  }
}
