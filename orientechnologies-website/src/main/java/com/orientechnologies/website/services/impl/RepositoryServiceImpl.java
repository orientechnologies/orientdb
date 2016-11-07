package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.configuration.GitHubConfiguration;
import com.orientechnologies.website.daemon.IssueAlignDaemon;
import com.orientechnologies.website.events.EventManager;
import com.orientechnologies.website.events.IssueCreatedEvent;
import com.orientechnologies.website.events.IssueEscalateEvent;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.HasIssue;
import com.orientechnologies.website.model.schema.HasLabel;
import com.orientechnologies.website.model.schema.HasMilestone;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.repository.*;
import com.orientechnologies.website.security.OSecurityManager;
import com.orientechnologies.website.services.*;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * Created by Enrico Risa on 21/10/14.
 */
@Service
public class RepositoryServiceImpl implements RepositoryService {

  @Autowired
  protected EventManager           eventManager;

  @Autowired
  protected EventService           eventService;
  @Autowired
  protected AutoAssignService      autoAssignService;

  @Autowired
  private OrientDBFactory          dbFactory;
  @Autowired
  protected RepositoryRepository   repositoryRepository;

  @Autowired
  protected IssueRepository        issueRepository;

  @Autowired
  protected IssueService           issueService;

  @Autowired
  protected UserRepository         userRepo;

  @Autowired
  protected UserService            userService;

  @Autowired
  protected EventRepository        eventRepository;

  protected RepositoryService      githubService;

  @Autowired
  protected OrganizationRepository organizationRepo;

  @Autowired
  protected EnvironmentRepository  environmentRepository;

  @Autowired
  protected IssueAlignDaemon       alignDaemon;

  @Autowired
  protected OSecurityManager       securityManager;

  @Autowired
  protected GitHubConfiguration    gitHubConfiguration;

  @PostConstruct
  protected void init() {
    githubService = new RepositoryServiceGithub(this);
  }

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

  @Transactional
  @Override
  public Issue openIssue(Repository repository, IssueDTO issue) {

    String org = repository.getOrganization().getName();
    if (securityManager.isCurrentClient(org) && !securityManager.isCurrentSupport(org)) {
      return createPrivateIssue(repository, issue);
    }
    if (Boolean.TRUE.equals(issue.getConfidential())) {
      return createPrivateIssue(repository, issue);
    } else {
      return githubService.openIssue(repository, issue);
    }
  }

  @Transactional
  @Override
  public Issue patchIssue(Issue original, OUser user, IssueDTO issue) {

    if (Boolean.TRUE.equals(original.getConfidential())) {
      return patchPrivateIssue(original, issue, true);
    } else {
      patchPrivateIssue(original, issue, false);
      OUser user1;
      try {
        user1 = securityManager.botIfSupport(original.getRepository().getOrganization().getName());
      } catch (Exception e) {
        user1 = user;
      }
      return githubService.patchIssue(original, user1, issue);
    }
  }

  @Override
  public void escalateIssue(final Issue i) {
    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("issue", i);
        put("actor", securityManager.currentUser());
      }
    };
    eventManager.pushInternalEvent(IssueEscalateEvent.EVENT, params);
  }

  private Issue patchPrivateIssue(Issue original, IssueDTO issue, boolean skipGithub) {
    Repository r = original.getRepository();

    if (issue.getVersion() != null) {
      if (original.getVersion() == null || original.getVersion().getNumber() != issue.getVersion()) {
        handleVersion(r, original, issue.getVersion());
      }
    }
    if (issue.getMilestone() != null) {
      if (original.getMilestone() == null || original.getMilestone().getNumber() != issue.getMilestone()) {
        handleMilestone(r, original, issue.getMilestone());
      }
    }
    if (issue.getPriority() != null) {
      if (original.getPriority() == null || original.getPriority().getNumber() != issue.getPriority()) {
        handlePriority(r, original, issue.getPriority());
        original = issueRepository.save(original);
      }
    }

    if (issue.getScope() != null) {
      if (original.getScope() == null || original.getScope().getNumber() != issue.getScope()) {
        handleScope(r, original, issue.getScope());
      }
    }
    if (issue.getTitle() != null) {
      if (!original.getTitle().equals(issue.getTitle())) {
        handleChangeTitle(r, original, issue.getTitle());
        original = issueRepository.save(original);
      }
    }
    if (issue.getBody() != null) {
      if (original.getBody() == null || (!original.getBody().equals(issue.getBody()))) {
        handleChangeBody(r, original, issue.getBody());
        original = issueRepository.save(original);
      }
    }
    if (issue.getEnvironment() != null) {
      original.setEnvironment(issue.getEnvironment());
      handleEnvironment(r, original, issue.getEnvironment());
      original = issueRepository.save(original);
    }

    if (issue.getClient() != null) {
      handleClient(r, original, issue.getClient());
    }
    if (skipGithub) {
      if (issue.getAssignee() != null) {
        if (original.getAssignee() == null || !original.getAssignee().getName().equalsIgnoreCase(issue.getAssignee())) {
          handleAssignee(original, issue.getAssignee());
        }
      }
      if (issue.getState() != null) {

        if (issue.getState().equalsIgnoreCase("OPEN")) {
          original.setClosedAt(null);
          original.setUpdatedAt(new Date());
        }
        if (issue.getState().equalsIgnoreCase("CLOSED")) {
          Date d = new Date();
          original.setClosedAt(d);
          original.setUpdatedAt(d);
        }
        if (!original.getState().equals(issue.getState())) {
          original = issueService.changeState(original, issue.getState(), null, true);
        }
      }
    }
    return original;
  }

  private Issue createPrivateIssue(Repository repository, final IssueDTO issue) {
    String assignee = issue.getAssignee();
    Integer milestoneId = issue.getMilestone();
    Integer versionId = issue.getVersion();
    Integer priorityId = issue.getPriority();
    Integer scope = issue.getScope();
    Integer client = issue.getClient();
    Environment env = issue.getEnvironment();
    Issue issueDomain = new Issue();
    issueDomain.setConfidential(true);
    issueDomain.setTitle(issue.getTitle());
    issueDomain.setBody(issue.getBody());
    Date createdAt = new Date();
    issueDomain.setCreatedAt(createdAt);
    issueDomain.setSlaAt(createdAt);
    issueDomain.setState(Issue.IssueState.OPEN.toString());
    issueDomain = issueRepository.save(issueDomain);
    createIssue(repository, issueDomain);
    OUser user = SecurityHelper.currentUser();
    issueService.changeUser(issueDomain, user);
    handleMilestone(repository, issueDomain, milestoneId);
    handleVersion(repository, issueDomain, versionId);
    handleScope(repository, issueDomain, scope);
    handleClient(repository, issueDomain, client);
    handleLabels(repository, issueDomain, issue.getLabels());
    handleEnvironment(repository, issueDomain, env);
    handleAssignee(issueDomain, assignee);
    handlePriority(repository, issueDomain, priorityId);
    Issue issue1 = issueRepository.save(issueDomain);
    eventManager.pushInternalEvent(IssueCreatedEvent.EVENT, issue1);
    List<OUser> bots = organizationRepo.findBots(repository.getOrganization().getName());
    if (bots.size() > 0 && issue.getType() != null) {
      issueService.addLabels(issue1, new ArrayList<String>() {
        {
          add(issue.getType());
        }
      }, bots.iterator().next(), true, false);
    }
    return issue1;
  }

  private void handleChangeTitle(Repository r, Issue original, String title) {
    issueService.changeTitle(original, title);
  }

  private void handleChangeBody(Repository r, Issue original, String body) {
    original.setBody(body);
  }

  protected void handleEnvironment(Repository repository, Issue issue, Environment env) {

    if (env != null) {
      issueService.changeEnvironment(issue, env);
    }
  }

  protected void handleClient(Repository repository, Issue issue, Integer clientId) {
    if (clientId != null) {
      Client client = organizationRepo.findClient(repository.getOrganization().getName(), clientId);
      if (client != null) {
        issueService.changeClient(issue, client);
      }
    }
  }

  protected void handleScope(Repository repository, Issue issue, Integer scope) {
    if (scope != null) {
      Scope p = repositoryRepository.findScope(repository.getName(), scope);
      if (p != null) {
        issueService.changeScope(issue, p);
      }
    }
  }

  protected void handlePriority(Repository repository, Issue issue, Integer priority) {
    if (priority != null) {
      Priority p = organizationRepo.findPriorityByNumber(repository.getOrganization().getName(), priority);
      if (p != null) {
        issueService.changePriority(issue, p);
      }
    }
  }

  protected void handleVersion(Repository repository, Issue issue, Integer milestone) {
    if (milestone != null) {
      Milestone m = repositoryRepository.findMilestoneByRepoAndName(repository.getName(), milestone);
      if (m != null) {
        issueService.changeVersion(issue, m);
      }
    }
  }

  private void handleLabels(Repository repository, Issue issue, List<String> labelsList) {

    List<Label> labels = new ArrayList<Label>();
    for (String label : labelsList) {
      Label l = repositoryRepository.findLabelsByRepoAndName(repository.getName(), label);
      if (l != null) {
        labels.add(l);
      }
    }
    issueService.changeLabels(issue, labels, false);
  }

  protected void handleMilestone(Repository repository, Issue issue, Integer milestone) {

    if (milestone != null) {
      Milestone m = repositoryRepository.findMilestoneByRepoAndName(repository.getName(), milestone);
      if (m != null) {
        issueService.changeMilestone(issue, m, null, true);
      }
    }
  }

  private void handleAssignee(final Issue issue, String assigneeName) {
    if (assigneeName != null) {
      OUser assignee = userRepo.findUserByLogin(assigneeName);
      if (assignee != null) {
        issueService.assign(issue, assignee, null, true);
      }
    } else {
      autoAssignService.findAssignee(issue, new AutoAssignService.AutoAssign() {
        @Override
        public void assign(OUser actor, OUser assignee) {
          if (actor != null && assignee != null)
            issueService.assign(issue, assignee, actor, true);
        }
      });
    }
  }

  @Override
  public void addLabel(Repository repo, Label label) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(repo.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(label.getId()));
    orgVertex.addEdge(HasLabel.class.getSimpleName(), devVertex);
  }

  @Override
  public void addMilestone(Repository repo, Milestone milestone) {
    OrientGraph graph = dbFactory.getGraph();

    OrientVertex orgVertex = graph.getVertex(new ORecordId(repo.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(milestone.getId()));
    orgVertex.addEdge(HasMilestone.class.getSimpleName(), devVertex);
  }

  @Override
  public void syncRepository(final Repository repository) {

    new Thread(new Runnable() {
      @Override
      public void run() {
        alignDaemon.importRepository(repository);
      }
    }).start();
  }

  private void createHasIssueRelationship(Repository repository, Issue issue) {
    issue.setRepository(repository);
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(repository.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(issue.getId()));
    orgVertex.addEdge(HasIssue.class.getSimpleName(), devVertex);
  }
}
