package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.HasIssue;
import com.orientechnologies.website.model.schema.HasLabel;
import com.orientechnologies.website.model.schema.HasMilestone;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.repository.EventRepository;
import com.orientechnologies.website.repository.IssueRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.RepositoryService;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 21/10/14.
 */
@Service
public class RepositoryServiceImpl implements RepositoryService {

  @Autowired
  private OrientDBFactory        dbFactory;
  @Autowired
  protected RepositoryRepository repositoryRepository;

  @Autowired
  protected IssueRepository      issueRepository;

  @Autowired
  protected IssueService         issueService;

  @Autowired
  protected UserRepository       userRepo;

  @Autowired
  protected EventRepository      eventRepository;

  @Override
  public Repository createRepo(String name, String description) {
    Repository repo = new Repository();
    repo.setName(name);
    repo.setDescription(description);
    return repositoryRepository.save(repo);
  }

  @Override
  public void createIssue(Repository repository, Issue issue) {
    createHasIssueRelationship(repository, issue);
  }

  @Override
  public Issue openIssue(Repository repository, Issue issue) {
    if (Boolean.TRUE.equals(issue.getConfidential())) {
      return createPrivateIssue(repository, issue);
    } else {
      return createPublicIssue(repository, issue);
    }
  }

  private Issue createPrivateIssue(Repository repository, Issue issue) {
    String assignee = issue.getAssignee() != null ? issue.getAssignee().getName() : null;
    Integer milestoneId = issue.getMilestone().getNumber();
    Integer versionId = issue.getVersion().getNumber();
    issue.setCreatedAt(new Date());
    issue.setState("open");
    issue = issueRepository.save(issue);
    createIssue(repository, issue);
    User user = SecurityHelper.currentUser();
    issueService.changeUser(issue, user);
    handleAssignee(issue, assignee);
    handleMilestone(repository, issue, milestoneId);
    handleVersion(repository, issue, versionId);
    handleLabels(repository, issue);
    return issue;
  }

  private void handleVersion(Repository repository, Issue issue, Integer milestone) {
    if (milestone != null) {
      Milestone m = repositoryRepository.findMilestoneByRepoAndName(repository.getName(), milestone);
      if (m != null) {
        issueService.changeVersion(issue, m);
        IssueEvent e = new IssueEvent();
        e.setCreatedAt(new Date());
        e.setEvent("versioned");
        e.setMilestone(m);
        e.setActor(SecurityHelper.currentUser());
        e = (IssueEvent) eventRepository.save(e);
        issueService.fireEvent(issue, e);
      }
    }
  }

  private void handleLabels(Repository repository, Issue issue) {

    List<Label> labels = new ArrayList<Label>();
    for (Label label : issue.getLabels()) {
      Label l = repositoryRepository.findLabelsByRepoAndName(repository.getName(), label.getName());
      if (l != null) {
        labels.add(l);
      }
    }
    issueService.changeLabels(issue, labels, false);
  }

  private void handleMilestone(Repository repository, Issue issue, Integer milestone) {

    if (milestone != null) {
      Milestone m = repositoryRepository.findMilestoneByRepoAndName(repository.getName(), milestone);
      if (m != null) {
        issueService.changeMilestone(issue, m);
        IssueEvent e = new IssueEvent();
        e.setCreatedAt(new Date());
        e.setEvent("milestoned");
        e.setActor(SecurityHelper.currentUser());
        e = (IssueEvent) eventRepository.save(e);
        issueService.fireEvent(issue, e);
      }
    }
  }

  private void handleAssignee(Issue issue, String assigneeName) {
    User assignee = userRepo.findUserByLogin(assigneeName);
    if (assignee != null) {
      issueService.changeAssignee(issue, assignee);
      IssueEvent e = new IssueEvent();
      e.setCreatedAt(new Date());
      e.setEvent("assigned");
      e.setAssegnee(assignee);
      e.setActor(SecurityHelper.currentUser());
      e = (IssueEvent) eventRepository.save(e);
      issueService.fireEvent(issue, e);
    }
  }

  private Issue createPublicIssue(Repository repository, Issue issue) {
    return null;
  }

  @Override
  public void addLabel(Repository repo, Label label) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(repo.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(label.getId()));
    orgVertex.addEdge(HasLabel.class.getSimpleName(), devVertex);
  }

  @Override
  public void addMilestone(Repository repo, Milestone milestone) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(repo.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(milestone.getId()));
    orgVertex.addEdge(HasMilestone.class.getSimpleName(), devVertex);
  }

  private void createHasIssueRelationship(Repository repository, Issue issue) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(repository.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(issue.getId()));
    orgVertex.addEdge(HasIssue.class.getSimpleName(), devVertex);
  }
}
