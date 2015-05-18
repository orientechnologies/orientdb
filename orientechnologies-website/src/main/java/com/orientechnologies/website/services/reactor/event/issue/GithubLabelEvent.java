package com.orientechnologies.website.services.reactor.event.issue;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.github.GLabel;
import com.orientechnologies.website.model.schema.OIssue;
import com.orientechnologies.website.model.schema.ORepository;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.*;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.RepositoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

/**
 * Created by Enrico Risa on 14/11/14.
 */

@Component
public class GithubLabelEvent implements GithubIssueEvent {

  @Autowired
  private RepositoryRepository   repositoryRepository;

  @Autowired
  private EventRepository        eventRepository;

  @Autowired
  private UserRepository         userRepository;

  @Autowired
  private IssueService           issueService;

  @Autowired
  private LabelRepository        labelRepository;

  @Autowired
  private RepositoryService      repositoryService;

  @Autowired
  private OrganizationRepository orgRepository;

  @Override
  public void handle(String evt, ODocument payload) {

    try {
      ODocument label = payload.field("label");
      ODocument issue = payload.field("issue");
      ODocument organization = payload.field("organization");
      ODocument repository = payload.field("repository");
      String organizationName = organization.field("login");

      final GLabel label1 = GLabel.fromDoc(label);
      String repoName = repository.field(ORepository.NAME.toString());
      Integer issueNumber = issue.field(OIssue.NUMBER.toString());

      Issue issueDto = repositoryRepository.findIssueByRepoAndNumber(repoName, issueNumber);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      Label l = repositoryRepository.findLabelsByRepoAndName(repoName, label1.getName());
      if (l == null) {
        Repository r = orgRepository.findOrganizationRepository(organizationName, repoName);
        l = new Label();
        l.setColor(label1.getColor());
        l.setName(label1.getName());
        l = labelRepository.save(l);
        repositoryService.addLabel(r, l);
      }
      issueService.addLabels(issueDto, new ArrayList<String>() {
        {
          add(label1.getName());
        }
      }, findUser(payload), true, false);
    } catch (Throwable e) {
      OLogManager.instance().warn(this, "Error labeling issue :" + payload.toJSON(), e);
    }
  }

  @Override
  public String handleWhat() {
    return "labeled";
  }

  protected OUser findUser(ODocument payload) {
    ODocument sender = payload.field("sender");
    String login = sender.field("login");
    Integer id = sender.field("id");
    return userRepository.findUserOrCreateByLogin(login, id.longValue());
  }
}
