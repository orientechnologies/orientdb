package com.orientechnologies.website.services.reactor.event.issue;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.github.GIssue;
import com.orientechnologies.website.github.GMilestone;
import com.orientechnologies.website.model.schema.ORepository;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Milestone;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.*;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.RepositoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 14/11/14.
 */

@Component
public class GithubOpenIssueEvent implements GithubIssueEvent {

  @Autowired
  private RepositoryRepository   repositoryRepository;

  @Autowired
  private EventRepository        eventRepository;

  @Autowired
  private UserRepository         userRepository;

  @Autowired
  private IssueService           issueService;

  @Autowired
  private IssueRepository        issueRepository;
  @Autowired
  private RepositoryService      repositoryService;

  @Autowired
  private MilestoneRepository    milestoneRepository;

  @Autowired
  private OrganizationRepository orgRepository;

  @Override
  public void handle(String evt, ODocument payload) {

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    ODocument issue = payload.field("issue");
    ODocument repository = payload.field("repository");
    ODocument organization = payload.field("organization");
    OUser user = findUser(issue, "user");
    String repoName = repository.field(ORepository.NAME.toString());
    String organizationName = organization.field("login");

    GIssue gIssue = GIssue.fromDoc(issue);
    Issue issue1 = repositoryRepository.findIssueByRepoAndNumber(repoName, gIssue.getNumber());
    if (issue1 != null) {
      return;
    }
    Repository r = orgRepository.findOrganizationRepository(organizationName, repoName);
    GMilestone m = gIssue.getMilestone();
    Milestone milestone = null;
    if (m != null) {
      milestone = repositoryRepository.findMilestoneByRepoAndName(repoName, m.getNumber());
      if (milestone == null) {
        milestone = new Milestone();
        milestone.setNumber(m.getNumber());
        milestone.setTitle(m.getTitle());
        milestone.setDescription(m.getDescription());
        milestone.setState(m.getState().name());
        milestone.setCreatedAt(m.getCreatedAt());
        milestone.setDueOn(m.getDueOn());
        milestone = milestoneRepository.save(milestone);
        repositoryService.addMilestone(r, milestone);
      }
    }
    Issue i = new Issue();
    i.setCreatedAt(gIssue.getCreatedAt());
    i.setTitle(gIssue.getTitle());
    i.setBody(gIssue.getBody());
    i.setNumber(gIssue.getNumber());
    i.setConfidential(false);
    i.setState(gIssue.getState().name());
    i = issueRepository.save(i);
    issueService.changeUser(i, user);
    repositoryService.createIssue(r, i);
    if (milestone != null)
      issueService.changeMilestone(i, milestone, user, true);

  }

  @Override
  public String handleWhat() {
    return "opened";
  }

  protected OUser findUser(ODocument payload, String field) {
      ODocument sender = payload.field(field);
    String login = sender.field("login");
    Integer id = sender.field("id");
    return userRepository.findUserOrCreateByLogin(login, id.longValue());
  }
}
