package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.repository.CommentRepository;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.services.IssueService;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Service
public class IssueServiceImpl implements IssueService {

  @Autowired
  private OrientDBFactory     dbFactory;

  @Autowired
  protected CommentRepository commentRepository;

  @Override
  public void commentIssue(Issue issue, Comment comment) {
    createCommentRelationship(issue, comment);
  }

  @Override
  public Comment createNewCommentOnIssue(Issue issue, Comment comment) {

    comment.setUser(SecurityHelper.currentUser());
    comment.setCreatedAt(new Date());

    comment = commentRepository.save(comment);
    commentIssue(issue, comment);
    return comment;
  }

  @Override
  public void changeMilestone(Issue issue, Milestone milestone) {
    createMilestoneRelationship(issue, milestone);
  }

  @Override
  public void changeLabels(Issue issue, List<Label> labels) {
    createLabelsRelationship(issue, labels);
  }

  @Override
  public void fireEvent(Issue issue, Event e) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(issue.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(e.getId()));

    orgVertex.addEdge(HasEvent.class.getSimpleName(), devVertex);
  }

  @Override
  public void changeUser(Issue issue, User user) {
    createUserRelationship(issue, user);

  }

  private void createUserRelationship(Issue issue, User user) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(issue.getId()));

    for (Edge edge : orgVertex.getEdges(Direction.IN, HasOpened.class.getSimpleName())) {
      edge.remove();
    }

    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(user.getId()));
    devVertex.addEdge(HasOpened.class.getSimpleName(), orgVertex);
  }

  @Override
  public void changeAssignee(Issue issue, User user) {
    createAssigneeRelationship(issue, user);

  }

  @Override
  public void changeVersion(Issue issue, Milestone milestone) {
    createVersionRelationship(issue, milestone);
  }

  private void createAssigneeRelationship(Issue issue, User user) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(issue.getId()));

    for (Edge edge : orgVertex.getEdges(Direction.OUT, IsAssigned.class.getSimpleName())) {
      edge.remove();
    }

    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(user.getId()));
    orgVertex.addEdge(IsAssigned.class.getSimpleName(), devVertex);
  }

  private void createLabelsRelationship(Issue issue, List<Label> labels) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(issue.getId()));

    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasLabel.class.getSimpleName())) {
      edge.remove();
    }

    for (Label label : labels) {
      OrientVertex devVertex = new OrientVertex(graph, new ORecordId(label.getId()));
      orgVertex.addEdge(HasLabel.class.getSimpleName(), devVertex);
    }

  }

  private void createVersionRelationship(Issue issue, Milestone milestone) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(issue.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(milestone.getId()));
    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasVersion.class.getSimpleName())) {
      edge.remove();
    }

    orgVertex.addEdge(HasVersion.class.getSimpleName(), devVertex);
  }

  private void createMilestoneRelationship(Issue issue, Milestone milestone) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(issue.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(milestone.getId()));
    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasMilestone.class.getSimpleName())) {
      edge.remove();
    }

    orgVertex.addEdge(HasMilestone.class.getSimpleName(), devVertex);
  }

  private void createCommentRelationship(Issue issue, Comment comment) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(issue.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(comment.getId()));
    orgVertex.addEdge(HasEvent.class.getSimpleName(), devVertex);
  }
}
