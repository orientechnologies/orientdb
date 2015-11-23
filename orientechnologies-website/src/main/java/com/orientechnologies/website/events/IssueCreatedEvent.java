package com.orientechnologies.website.events;

import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.AttachmentRepository;
import com.orientechnologies.website.repository.IssueRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Component;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;
import reactor.event.Event;

import java.util.*;

/**
 * Created by Enrico Risa on 30/12/14.
 */
@Component
public class IssueCreatedEvent extends EventInternal<Issue> {

  @Autowired
  @Lazy
  protected JavaMailSenderImpl   sender;

  @Autowired
  protected AppConfig            config;

  @Autowired
  private IssueRepository        issueRepository;

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

    issue = organizationRepository.findSingleOrganizationIssueByNumber(issue.getRepository().getOrganization().getName(),
        issue.getIid());

    if (issue != null) {

      if (Boolean.TRUE.equals(issue.getConfidential()) && issue.getClient() != null) {
        try {
          attachmentRepository.createIssueFolder(issue.getRepository().getOrganization().getName(), issue);
        } catch (Exception e) {

        }
      }

      Context context = new Context();
      fillContextVariable(context, issue);
      String htmlContent = templateEngine.process("newIssue.html", context);

      OUser owner = issue.getScope() != null ? issue.getScope().getOwner() : null;
      OUser user = issue.getUser();

      Set<String> dests = new HashSet<String>();
      List<OUser> members = issue.getScope() != null ? issue.getScope().getMembers() : new ArrayList<OUser>();
      if (owner != null) {
        boolean found = false;
        for (OUser member : members) {
          if (member.getName().equals(owner.getName())) {
            found = true;
          }
        }
        if (!found && !user.getName().equals(owner.getName())) {
          members.add(owner);
        }
      }

      List<OUser> issueActors = null;
      if (!Boolean.TRUE.equals(issue.getConfidential())) {
        issueActors = issueRepository.findToNotifyActors(issue);
        members.addAll(issueActors);
        members.addAll(issueRepository.findToNotifyActorsWatching(issue));

      } else {
        issueActors = issueRepository.findToNotifyPrivateActors(issue);
        members.addAll(issueActors);
      }
      String[] actorsEmail = getActorsEmail(owner, members, issueActors);
      dests.addAll(Arrays.asList(actorsEmail));
      if (dests.size() > 0) {
        for (String actor : dests) {
          SimpleMailMessage mailMessage = new SimpleMailMessage();
          try {
            mailMessage.setTo(actor);
            mailMessage.setFrom(issue.getUser().getName());
            mailMessage.setSubject(fillSubjectTags(issue));
            mailMessage.setText(htmlContent);
            sender.send(mailMessage);
          } catch (Exception e) {

          }
        }
      }
      if (issue.getClient() != null) {
        sendSupportMail(sender, issue, htmlContent, true);
      }
    }
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
