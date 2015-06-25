package com.orientechnologies.website.services.reactor.event.issue;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.OIssue;
import com.orientechnologies.website.model.schema.ORepository;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OLabel;
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
public class GithubUnLabelEvent implements GithubIssueEvent {

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

    ODocument label = payload.field("label");
    ODocument issue = payload.field("issue");
    ODocument repository = payload.field("repository");

    String repoName = repository.field(ORepository.NAME.toString());
    Integer issueNumber = issue.field(OIssue.NUMBER.toString());
    String labelName = label.field(OLabel.NAME.toString());
    Issue issueDto = repositoryRepository.findIssueByRepoAndNumber(repoName, issueNumber);

    issueService.removeLabel(issueDto, labelName, findUser(payload), false);

  }

  @Override
  public String handleWhat() {
    return "unlabeled";
  }

  protected OUser findUser(ODocument payload) {
    ODocument sender = payload.field("sender");
    String login = sender.field("login");
    Number id = sender.field("id");
    return userRepository.findUserOrCreateByLogin(login, id.longValue());
  }

  @Override
  public String formantPayload(ODocument payload) {
    ODocument label = payload.field("label");
    return label.field("name");
  }
}
