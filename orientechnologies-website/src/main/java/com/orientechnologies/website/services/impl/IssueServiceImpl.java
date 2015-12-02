package com.orientechnologies.website.services.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.configuration.GitHubConfiguration;
import com.orientechnologies.website.events.*;
import com.orientechnologies.website.github.GIssue;
import com.orientechnologies.website.github.GRepo;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.*;
import com.orientechnologies.website.security.OSecurityManager;
import com.orientechnologies.website.services.EventService;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.SlaService;
import com.orientechnologies.website.services.UserService;
import com.orientechnologies.website.services.reactor.GitHubIssueImporter;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Service
public class IssueServiceImpl implements IssueService {

  public static String           WAIT_FOR_REPLY = "waiting reply";
  public static String           IN_PROGRESS    = "in progress";
  @Autowired
  private OrientDBFactory        dbFactory;

  @Autowired
  protected GitHubConfiguration  gitHubConfiguration;
  @Autowired
  protected CommentRepository    commentRepository;

  @Autowired
  protected EventManager         eventManager;

  @Autowired
  protected EventService         eventService;
  @Autowired
  protected RepositoryRepository repoRepository;
  @Autowired
  private EventRepository        eventRepository;
  @Autowired
  private IssueRepository        issueRepository;

  @Autowired
  private GitHubIssueImporter    issueImporter;

  private IssueService           githubIssueService;

  @Autowired
  private OrganizationRepository organizationRepository;
  @Autowired
  private ContractRepository     contractRepository;
  @Autowired
  protected SlaService           slaService;

  @Autowired
  protected UserService          userService;

  @Autowired
  protected OSecurityManager     securityManager;

  @PostConstruct
  protected void init() {
    githubIssueService = new IssueServiceGithub(this);
  }

  public void commentIssue(Issue issue, Comment comment) {
    commentIssue(issue, comment, false);
  }

  @Override
  public void commentIssue(Issue issue, Comment comment, boolean bot) {
    createCommentRelationship(issue, comment);

    if (!bot) {

      if (issue.getClient() != null) {
        Client client = userService.getClient(comment.getUser(), issue.getRepository().getOrganization().getName());
        if (client != null && client.getClientId() == issue.getClient().getClientId() && issue.getDueTime() == null
            && !issue.isClosed()) {
          OUser actor = securityManager.bot(issue.getRepository().getOrganization().getName());
          if (!isInProgress(issue)) {
            issue.setSlaAt(new Date());
            changeSlaDueTime(issue, actor, issue.getPriority());
          }
          issue = issueRepository.save(issue);
          removeLabel(issue, WAIT_FOR_REPLY, actor, !Boolean.TRUE.equals(issue.getConfidential()));
        }
      } else {
        if (issue.getUser().getName().equals(comment.getUser().getUsername())) {
          removeLabel(issue, WAIT_FOR_REPLY, securityManager.bot(issue.getRepository().getOrganization().getName()),
              !Boolean.TRUE.equals(issue.getConfidential()));
        }
      }
    }
  }

  @Transactional
  @Override
  public Comment createNewCommentOnIssue(Issue issue, Comment comment, OUser actor) {

    if (Boolean.TRUE.equals(issue.getConfidential())) {

      if (actor != null) {
        comment.setUser(actor);
      } else {
        comment.setUser(SecurityHelper.currentUser());
      }

      comment.setCreatedAt(new Date());

      comment = commentRepository.save(comment);
      commentIssue(issue, comment);

      comment.setOwner(issue);
      eventManager.pushInternalEvent(IssueCommentedEvent.EVENT, comment);
      return comment;
    } else {
      return githubIssueService.createNewCommentOnIssue(issue, comment, actor);
    }
  }

  @Override
  public void changeLabels(Issue issue, List<Label> labels, boolean replace) {
    createLabelsRelationship(issue, labels, replace);
  }

  @Transactional
  @Override
  public List<Label> addLabels(Issue issue, List<String> labels, OUser actor, boolean fire, boolean remote) {

    if (!remote) {

      return labelIssue(issue, labels, actor, fire);
    } else {
      return githubIssueService.addLabels(issue, labels, actor, fire, remote);
    }

  }

  private boolean isStopSla(String label) {
    return WAIT_FOR_REPLY.equals(label) || IN_PROGRESS.equals(label);
  }

  private boolean isInProgress(Issue issue) {

    List<Label> collect = issue.getLabels().stream().filter(i -> IN_PROGRESS.equals(i.getName())).collect(Collectors.toList());
    return collect.size() > 0;
  }

  private List<Label> labelIssue(Issue issue, List<String> labels, OUser actor, boolean fire) {
    List<Label> lbs = new ArrayList<Label>();

    for (String label : labels) {
      Label l = repoRepository.findLabelsByRepoAndName(issue.getRepository().getName(), label);
      if (l != null) {
        lbs.add(l);
        if (isStopSla(l.getName()) && issue.getDueTime() != null) {
          removeSlaCounting(issue, actor);
        }
      }

    }
    changeLabels(issue, lbs, false);

    if (fire) {
      for (Label lb : lbs) {
        fireLabelEvent(issue, actor, lb);
      }
    }
    return lbs;
  }

  private void fireLabelEvent(Issue issue, OUser actor, Label lb) {
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

  private void removeSlaCounting(Issue issue, OUser actor) {
    issue.setDueTime(null);
    issueRepository.save(issue);
    IssueEventInternal e = new IssueEventInternal();
    e.setCreatedAt(new Date());
    e.setEvent("slaStopped");
    e.setPriority(issue.getPriority());
    e.setActor(actor != null ? actor : SecurityHelper.currentUser());
    e.setSecret(true);
    e.setTime(issue.getDueTime());
    e = (IssueEventInternal) eventRepository.save(e);
    fireEvent(issue, e);
  }

  @Transactional
  @Override
  public void removeLabel(Issue issue, String label, OUser actor, boolean remote) {
    Label l = repoRepository.findLabelsByRepoAndName(issue.getRepository().getName(), label);
    if (!remote) {

      if (l != null) {
        Edge e = removeLabelRelationship(issue, l);

        if (e != null) {
          e.remove();
          fireUnLabelEvent(issue, actor, l);
        }
      }
    } else {
      if (l != null) {
        if (removeLabelRelationship(issue, l) != null) {
          githubIssueService.removeLabel(issue, label, actor, remote);
        }
      }
    }
  }

  private void fireUnLabelEvent(Issue issue, OUser actor, Label l) {
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

    if (issue.getAssignee() != null) {
      removeAssigneeRelationship(issue, issue.getAssignee());
    }
    createAssigneeRelationship(issue, assignee);

    if (fire) {
      IssueEvent e = fireAssignEvent(issue, assignee, actor);
      eventManager.pushInternalEvent(IssueAssignedEvent.EVENT, e);
    }
  }

  private IssueEvent fireAssignEvent(Issue issue, OUser assignee, OUser actor) {
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
    return e;
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
  public void changeMilestone(Issue issue, Milestone milestone, OUser actor, boolean fire) {
    Milestone oldMileston = issue.getMilestone();
    createMilestoneRelationship(issue, milestone);
    if (fire) {
      IssueEvent e = new IssueEvent();
      e.setCreatedAt(new Date());
      e.setEvent("milestoned");
      e.setMilestone(milestone);
      if (actor == null) {
        e.setActor(SecurityHelper.currentUser());
      } else {
        e.setActor(actor);
      }
      e = (IssueEvent) eventRepository.save(e);
      fireEvent(issue, e);
      if (oldMileston != null) {
        e = new IssueEventInternal();
        e.setCreatedAt(new Date());
        if (actor == null) {
          e.setActor(SecurityHelper.currentUser());
        } else {
          e.setActor(actor);
        }
        e.setEvent("demilestoned");
        e.setMilestone(oldMileston);
        e = (IssueEventInternal) eventRepository.save(e);
        fireEvent(issue, e);
      }
    }
  }

  @Override
  public void changeVersion(Issue issue, Milestone milestone) {
    Milestone oldVersion = issue.getVersion();
    createVersionRelationship(issue, milestone);
    IssueEventInternal e = new IssueEventInternal();
    e.setCreatedAt(new Date());
    e.setEvent("versioned");
    e.setVersion(milestone);
    e.setActor(SecurityHelper.currentUser());
    e = (IssueEventInternal) eventRepository.save(e);
    fireEvent(issue, e);
    if (oldVersion != null) {
      e = new IssueEventInternal();
      e.setCreatedAt(new Date());
      e.setEvent("unversioned");
      e.setVersion(oldVersion);
      e.setActor(SecurityHelper.currentUser());
      e = (IssueEventInternal) eventRepository.save(e);
      fireEvent(issue, e);
    }
  }

  @Override
  public void changeSlaDueTime(Issue issue, OUser actor, Priority priority) {

    if (issue.getClient() != null && priority != null) {

      List<Contract> contracts = organizationRepository.findClientContracts(issue.getRepository().getOrganization().getName(),
          issue.getClient().getClientId());

      // TODO WHICH CONTRACT?
      if (contracts.size() > 0) {

        try {
          Contract c = contracts.iterator().next();
          issue.setDueTime(slaService.calculateDueTime(issue.getSlaAt(), c, priority));
          IssueEventInternal e = new IssueEventInternal();
          e.setCreatedAt(new Date());
          e.setEvent("slaStarted");
          e.setPriority(priority);
          e.setActor(actor != null ? actor : SecurityHelper.currentUser());
          e.setSecret(true);
          e.setTime(issue.getDueTime());
          e = (IssueEventInternal) eventRepository.save(e);
          fireEvent(issue, e);
        } catch (Exception ex) {
          OLogManager.instance().warn(this, "Error Calculating sla", ex);
        }

      }

    }

  }

  @Override
  public void changePriority(Issue issue, Priority priority) {
    Priority oldPriority = issue.getPriority();
    createPriorityRelationship(issue, priority);
    IssueEventInternal e = new IssueEventInternal();
    e.setCreatedAt(new Date());
    e.setEvent("prioritized");
    e.setPriority(priority);
    e.setActor(SecurityHelper.currentUser());
    e = (IssueEventInternal) eventRepository.save(e);
    fireEvent(issue, e);
    if (oldPriority != null) {
      e = new IssueEventInternal();
      e.setCreatedAt(new Date());
      e.setEvent("unprioritized");
      e.setPriority(oldPriority);
      e.setActor(SecurityHelper.currentUser());
      e = (IssueEventInternal) eventRepository.save(e);
      fireEvent(issue, e);
    }
    changeSlaDueTime(issue, securityManager.bot(issue.getRepository().getOrganization().getName()), priority);
  }

  @Override
  public void changeScope(Issue issue, Scope scope) {
    Scope oldScope = issue.getScope();
    issue.setScope(scope);
    createScopeRelationshipt(issue, scope);
    IssueEventInternal e = new IssueEventInternal();
    e.setCreatedAt(new Date());
    e.setEvent("scoped");
    e.setScope(scope);
    e.setActor(SecurityHelper.currentUser());
    e = (IssueEventInternal) eventRepository.save(e);
    fireEvent(issue, e);
    if (oldScope != null) {
      e = new IssueEventInternal();
      e.setCreatedAt(new Date());
      e.setEvent("unscoped");
      e.setScope(oldScope);
      e.setActor(SecurityHelper.currentUser());
      e = (IssueEventInternal) eventRepository.save(e);
      fireEvent(issue, e);
    }
  }

  @Override
  public void changeClient(Issue issue, Client client) {
    issue.setClient(client);
    createClientRelationship(issue, client);
  }

  @Override
  public void changeEnvironment(Issue issue, Environment e) {
    embedEnvironment(issue, e);
  }

  @Transactional
  @Override
  public Comment patchComment(Issue issue, String commentUUID, Comment comment) {

    if (!Boolean.TRUE.equals(issue.getConfidential())) {
      githubIssueService.patchComment(issue, commentUUID, comment);
    }
    Comment c = commentRepository.findByIssueAndCommentUUID(issue, commentUUID);
    c.setBody(comment.getBody());
    c.setUpdatedAt(new Date());
    c = commentRepository.save(c);
    return c;
  }

  @Override
  public Comment deleteComment(Issue issue, String commentUUID, Comment comment) {

    Comment c = commentRepository.findByIssueAndCommentUUID(issue, commentUUID);
    if (!Boolean.TRUE.equals(issue.getConfidential())) {
      githubIssueService.deleteComment(issue, commentUUID, c);
    }
    commentRepository.delete(c);
    return c;
  }

  @Override
  public List<OUser> findInvolvedActors(Issue issue) {
    return issueRepository.findInvolvedActors(issue);
  }

  @Override
  public void clearComments(Issue issue) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));

    List<OrientVertex> toRemove = new ArrayList<OrientVertex>();
    for (Vertex v : orgVertex.getVertices(Direction.OUT, HasEvent.class.getSimpleName())) {
      OrientVertex ov = (OrientVertex) v;
      String type = ov.getType().getName();
      if (Comment.class.getSimpleName().equalsIgnoreCase(type)) {
        toRemove.add(ov);
      }
    }
    for (OrientVertex vertex : toRemove) {
      vertex.remove();
    }
  }

  @Override
  public void changeTitle(Issue issue, String title) {
    String oldTitle = issue.getTitle();
    issue.setTitle(title);

    String evt = "renamed";
    IssueEvent e = new IssueEvent();
    e.setCreatedAt(new Date());
    e.setEvent(evt);
    e.setActor(SecurityHelper.currentUser());
    e.setFrom(oldTitle);
    e.setTo(title);
    e = (IssueEvent) eventRepository.save(e);
    fireEvent(issue, e);
  }

  private void embedEnvironment(Issue issue, Environment environment) {
    issue.setEnvironment(environment);
    IssueEventInternal e = new IssueEventInternal();
    e.setCreatedAt(new Date());
    e.setEvent("environmented");
    e.setEnvironment(environment);
    e.setActor(SecurityHelper.currentUser());
    e = (IssueEventInternal) eventRepository.save(e);
    fireEvent(issue, e);
  }

  @Override
  public Issue changeState(Issue issue, String state, OUser actor, boolean fire) {
    issue.setState(state);
    if (fire) {
      String evt = issue.getState().equalsIgnoreCase(Issue.IssueState.OPEN.toString()) ? "reopened" : "closed";
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

      if (evt.equals("reopened")) {
        eventManager.pushInternalEvent(IssueReopenEvent.EVENT, e);
        issue.setSlaAt(new Date());
        changeSlaDueTime(issue, actor, issue.getPriority());
        issue = issueRepository.save(issue);

      } else {
        eventManager.pushInternalEvent(IssueClosedEvent.EVENT, e);
        if (issue.getClient() != null && issue.getDueTime() != null)
          removeSlaCounting(issue, actor);
      }
    }

    return issueRepository.save(issue);
  }

  @Transactional
  @Override
  public Issue synchIssue(Issue issue, OUser user) {

    String token = user != null ? user.getToken() : SecurityHelper.currentToken();
    GitHub github = new GitHub(token, gitHubConfiguration);

    ODocument doc = new ODocument();
    String iPropertyValue = issue.getRepository().getOrganization().getName() + "/" + issue.getRepository().getName();
    doc.field("full_name", iPropertyValue);

    try {
      GRepo repo = github.repo(iPropertyValue, doc.toJSON());
      GIssue is = repo.getIssue(issue.getNumber());
      issueImporter.importLabels(repo.getLabels(), issue.getRepository());
      issueImporter.importMilestones(repo.getMilestones(), issue.getRepository());
      issueImporter.importSingleIssue(issue.getRepository(), is);

    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Issue conditionalSynchIssue(Issue issue, OUser user) {
    String token = user != null ? user.getToken() : SecurityHelper.currentToken();
    GitHub github = new GitHub(token, gitHubConfiguration);

    ODocument doc = new ODocument();
    String iPropertyValue = issue.getRepository().getOrganization().getName() + "/" + issue.getRepository().getName();
    doc.field("full_name", iPropertyValue);

    try {
      GRepo repo = github.repo(iPropertyValue, doc.toJSON());
      GIssue is = repo.isChangedIssue(issue.getNumber(), issue.getUpdatedAt());
      if (is != null) {
        issueImporter.importSingleIssue(issue.getRepository(), is);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public GIssue isChanged(Issue issue, OUser user) {
    return githubIssueService.isChanged(issue, user);
  }

  @Override
  public void clearEvents(Issue issue) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));

    for (Vertex v : orgVertex.getVertices(Direction.OUT, HasEvent.class.getSimpleName())) {
      OrientVertex ov = (OrientVertex) v;
      String type = ov.getType().getName();
      if (IssueEvent.class.getSimpleName().equalsIgnoreCase(type)) {
        v.remove();
      }
    }

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

  private Edge removeLabelRelationship(Issue issue, Label label) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    Edge toRemove = null;
    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasLabel.class.getSimpleName())) {
      Vertex v = edge.getVertex(Direction.IN);
      if (label.getName().equals(v.getProperty(OLabel.NAME.toString()))) {
        toRemove = edge;
      }

    }
    return toRemove;
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

  private void createClientRelationship(Issue issue, Client client) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(client.getId()));
    for (Edge edge : orgVertex.getEdges(Direction.IN, HasClient.class.getSimpleName())) {
      edge.remove();
    }
    devVertex.addEdge(HasClient.class.getSimpleName(), orgVertex);
  }

  private void createPriorityRelationship(Issue issue, Priority priority) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(priority.getId()));
    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasPriority.class.getSimpleName())) {
      edge.remove();
    }

    orgVertex.addEdge(HasPriority.class.getSimpleName(), devVertex);
  }

  private void createScopeRelationshipt(Issue issue, Scope scope) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(scope.getId()));
    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasScope.class.getSimpleName())) {
      edge.remove();
    }

    orgVertex.addEdge(HasScope.class.getSimpleName(), devVertex);
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
