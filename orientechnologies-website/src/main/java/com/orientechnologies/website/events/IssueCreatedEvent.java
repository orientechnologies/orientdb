package com.orientechnologies.website.events;

import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.AttachmentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;
import reactor.event.Event;

/**
 * Created by Enrico Risa on 30/12/14.
 */
@Component
public class IssueCreatedEvent extends EventInternal<Issue> {

  @Autowired
  protected AppConfig            config;

  @Autowired
  private SpringTemplateEngine   templateEngine;

  public static String           EVENT = "issue_created";

  @Autowired
  protected AttachmentRepository attachmentRepository;

  @Override
  public String event() {
    return EVENT;
  }

  @Override
  public void accept(Event<Issue> issueEvent) {

    Issue issue = issueEvent.getData();

    long iid = issue.getIid();

    String orgName = issue.getRepository().getOrganization().getName();

    issue = organizationRepository.findSingleOrganizationIssueByNumber(orgName, iid);
    if (issue != null) {
      sendOpenedIssueMail(issue);
    } else {
      logger.info("Cannot find issue : " + iid);
    }

  }

  private void sendOpenedIssueMail(Issue issue) {
    if (Boolean.TRUE.equals(issue.getConfidential()) && issue.getClient() != null) {
      try {
        attachmentRepository.createIssueFolder(issue.getRepository().getOrganization().getName(), issue);
      } catch (Exception e) {

      }
    }

    Context context = new Context();
    fillContextVariable(context, issue);
    String htmlContent = templateEngine.process("newIssue.html", context);

    OUser user = issue.getUser();

    sendEmail(issue, htmlContent, user,config);

    if (issue.getClient() != null) {
      sendSupportMail(sender, issue, htmlContent, true);
    }
    postHandle();
  }

  private void fillContextVariable(Context context, Issue issue) {
    context.setVariable("link", config.endpoint + "/#issues/" + issue.getIid());
    String body = "";
    if (issue.getBody() != null) {
      body = issue.getBody();
    }
    String user = "@" + issue.getUser().getName() + " opened issue #" + issue.getIid();
    context.setVariable("body", body);
    context.setVariable("user", user);
  }
}
