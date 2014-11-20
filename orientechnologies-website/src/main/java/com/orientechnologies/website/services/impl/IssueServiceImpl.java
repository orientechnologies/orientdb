package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.CommentRepository;
import com.orientechnologies.website.repository.EventRepository;
import com.orientechnologies.website.repository.IssueRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.services.IssueService;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Service
public class IssueServiceImpl implements IssueService {

  @Autowired
  private OrientDBFactory      dbFactory;

  @Autowired
  protected CommentRepository  commentRepository;

  @Autowired
  private RepositoryRepository repoRepository;
  @Autowired
  private EventRepository      eventRepository;
  @Autowired
  private IssueRepository      issueRepository;

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
  public void changeMilestone(Issue issue, Milestone milestone, OUser actor, boolean fire) {
    createMilestoneRelationship(issue, milestone);
    if (fire) {
      IssueEvent e = new IssueEvent();
      e.setCreatedAt(new Date());
      e.setEvent("milestoned");
      if (actor == null) {
        e.setActor(SecurityHelper.currentUser());
      } else {
        e.setActor(actor);
      }
      e = (IssueEvent) eventRepository.save(e);
      fireEvent(issue, e);
    }
  }

  @Override
  public void changeLabels(Issue issue, List<Label> labels, boolean replace) {
    createLabelsRelationship(issue, labels, replace);
  }

  @Override
  public List<Label> addLabels(Issue issue, List<String> labels, OUser actor, boolean fire) {

    List<Label> lbs = new ArrayList<Label>();

    for (String label : labels) {
      Label l = repoRepository.findLabelsByRepoAndName(issue.getRepository().getName(), label);
      lbs.add(l);
    }
    changeLabels(issue, lbs, false);

    if (fire) {
      for (Label lb : lbs) {
        IssueEvent e = new IssueEvent();
        e.setCreatedAt(new Date());
        e.setEvent("labeled");
        e.setLabel(lb);
        if (actor == null) {
          e.setActor(SecurityHelper.currentUser());
        } else {
          e.setActor(actor);
        }
        e = (IssueEvent) eventRepository.save(e);
        fireEvent(issue, e);
      }
    }
    return lbs;
  }

  @Override
  public void removeLabel(Issue issue, String label, OUser actor) {
    Label l = repoRepository.findLabelsByRepoAndName(issue.getRepository().getName(), label);
    if (l != null) {
      removeLabelRelationship(issue, l);
      IssueEvent e = new IssueEvent();
      e.setCreatedAt(new Date());
      e.setEvent("unlabeled");
      e.setLabel(l);
      if (actor == null) {
        e.setActor(SecurityHelper.currentUser());
      } else {
        e.setActor(actor);
      }
      e = (IssueEvent) eventRepository.save(e);
      fireEvent(issue, e);
    }
  }

  @Override
  public void fireEvent(Issue issue, Event e) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(e.getId()));

    orgVertex.addEdge(HasEvent.class.getSimpleName(), devVertex);
  }

  @Override
  public void changeUser(Issue issue, OUser user) {
    createUserRelationship(issue, user);

  }

  private void createUserRelationship(Issue issue, OUser user) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));

    for (Edge edge : orgVertex.getEdges(Direction.IN, HasOpened.class.getSimpleName())) {
      edge.remove();
    }

    OrientVertex devVertex = graph.getVertex(new ORecordId(user.getRid()));
    devVertex.addEdge(HasOpened.class.getSimpleName(), orgVertex);
  }

  @Override
  public void assign(Issue issue, OUser assignee, OUser actor, boolean fire) {

    createAssigneeRelationship(issue, assignee);

    if (fire) {
      IssueEvent e = new IssueEvent();
      e.setCreatedAt(new Date());
      e.setEvent("assigned");
      e.setAssignee(assignee);
      if (actor == null) {
        e.setActor(SecurityHelper.currentUser());
      } else {
        e.setActor(actor);
      }
      e = (IssueEvent) eventRepository.save(e);
      fireEvent(issue, e);
    }
  }

  @Override
  public void unassign(Issue issue, OUser assignee, OUser actor, boolean fire) {
    removeAssigneeRelationship(issue, assignee);
    if (fire) {
      IssueEvent e = new IssueEvent();
      e.setCreatedAt(new Date());
      e.setEvent("unassigned");
      e.setAssignee(assignee);
      if (actor == null) {
        e.setActor(SecurityHelper.currentUser());
      } else {
        e.setActor(actor);
      }
      e = (IssueEvent) eventRepository.save(e);
      fireEvent(issue, e);
    }
  }

  @Override
  public void changeVersion(Issue issue, Milestone milestone) {
    createVersionRelationship(issue, milestone);
  }

  @Override
  public Issue changeState(Issue issue, String state, OUser actor, boolean fire) {
    issue.setState(state);
    if (fire) {
      String evt = issue.getState().equalsIgnoreCase("open") ? "reopened" : "closed";
      IssueEvent e = new IssueEvent();
      e.setCreatedAt(new Date());
      e.setEvent(evt);
      if (actor == null) {
        e.setActor(SecurityHelper.currentUser());
      } else {
        e.setActor(actor);
      }
      e = (IssueEvent) eventRepository.save(e);
      fireEvent(issue, e);
    }

    return issueRepository.save(issue);
  }

  private void createAssigneeRelationship(Issue issue, OUser user) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));

    // for (Edge edge : orgVertex.getEdges(Direction.OUT, IsAssigned.class.getSimpleName())) {
    // edge.remove();
    // }

    if (user != null) {
      OrientVertex devVertex = graph.getVertex(new ORecordId(user.getRid()));
      orgVertex.addEdge(IsAssigned.class.getSimpleName(), devVertex);
    }
  }

  private void removeAssigneeRelationship(Issue issue, OUser user) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));

    for (Edge edge : orgVertex.getEdges(Direction.OUT, IsAssigned.class.getSimpleName())) {
      Vertex in = edge.getVertex(Direction.IN);
      if (in.getProperty(com.orientechnologies.website.model.schema.OUser.NAME.toString()).equals(user.getName())) {
        edge.remove();
      }
    }

  }

  private void removeLabelRelationship(Issue issue, Label label) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));

    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasLabel.class.getSimpleName())) {
      Vertex v = edge.getVertex(Direction.IN);
      if (label.getName().equals(v.getProperty(OLabel.NAME.toString()))) {
        edge.remove();
      }

    }

  }

  private void createLabelsRelationship(Issue issue, List<Label> labels, boolean replace) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));

    if (replace) {
      for (Edge edge : orgVertex.getEdges(Direction.OUT, HasLabel.class.getSimpleName())) {
        edge.remove();
      }
    }
    for (Label label : labels) {
      OrientVertex devVertex = graph.getVertex(new ORecordId(label.getId()));
      orgVertex.addEdge(HasLabel.class.getSimpleName(), devVertex);
    }

  }

  private void createVersionRelationship(Issue issue, Milestone milestone) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(milestone.getId()));
    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasVersion.class.getSimpleName())) {
      edge.remove();
    }

    orgVertex.addEdge(HasVersion.class.getSimpleName(), devVertex);
  }

  private void createMilestoneRelationship(Issue issue, Milestone milestone) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(milestone.getId()));
    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasMilestone.class.getSimpleName())) {
      edge.remove();
    }

    orgVertex.addEdge(HasMilestone.class.getSimpleName(), devVertex);
  }

  private void createCommentRelationship(Issue issue, Comment comment) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(comment.getId()));
    orgVertex.addEdge(HasEvent.class.getSimpleName(), devVertex);
  }
}
