package com.orientechnologies.website.services.reactor.event.issue;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.OIssue;
import com.orientechnologies.website.model.schema.ORepository;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.EventRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.IssueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 14/11/14.
 */

@Component
public class GithubAssignedEvent implements GithubIssueEvent {

  @Autowired
  private RepositoryRepository repositoryRepository;

  @Autowired
  private EventRepository      eventRepository;

  @Autowired
  private UserRepository       userRepository;

  @Autowired
  private IssueService         issueService;

  @Override
  public void handle(String evt, ODocument payload) {

    ODocument issue = payload.field("issue");
    ODocument repository = payload.field("repository");

    OUser sender = findUser(payload, "sender");
    OUser assignee = findUser(payload, "assignee");
    String repoName = repository.field(ORepository.NAME.toString());

    if (issue != null) {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Integer issueNumber = issue.field(OIssue.NUMBER.toString());
      Issue issueDto = repositoryRepository.findIssueByRepoAndNumber(repoName, issueNumber);
      issueService.assign(issueDto, assignee, sender, true);
    }
  }

  @Override
  public String handleWhat() {
    return "assigned";
  }

  protected OUser findUser(ODocument payload, String field) {
    ODocument sender = payload.field(field);
    String login = sender.field("login");
    Number id = sender.field("id");
    return userRepository.findUserOrCreateByLogin(login, id.longValue());
  }

  @Override
  public String formantPayload(ODocument payload) {
    ODocument sender = payload.field("assignee");
    return sender.field("login");
  }
}
