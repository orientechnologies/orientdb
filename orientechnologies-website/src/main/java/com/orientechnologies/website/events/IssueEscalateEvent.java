package com.orientechnologies.website.events;

import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.IssueRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Component;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;
import reactor.event.Event;

import java.util.Map;

/**
 * Created by Enrico Risa on 30/12/14.
 */
@Component
public class IssueEscalateEvent extends EventInternal<Map<String, Object>> {

  @Autowired
  @Lazy
  protected JavaMailSenderImpl sender;

  @Autowired
  protected AppConfig          config;

  @Autowired
  private IssueRepository      issueRepository;

  @Autowired
  private SpringTemplateEngine templateEngine;

  public static String         EVENT = "issue_escalate";

  @Override
  public String event() {
    return EVENT;
  }

  @Override
  public void accept(Event<Map<String, Object>> issueEvent) {

    Map<String, Object> data = issueEvent.getData();
    Issue issue = (Issue) data.get("issue");
    OUser user = (OUser) data.get("actor");
    Context context = new Context();
    fillContextVariable(context, issue, user);
    String htmlContent = templateEngine.process("escalate.html", context);
    SimpleMailMessage mailMessage = new SimpleMailMessage();
    mailMessage.setTo(config.escalateMil);
    mailMessage.setCc(config.escalateMilcc);
    mailMessage.setFrom(user.getName());
    mailMessage.setSubject(fillSubjectTags(issue));
    mailMessage.setText(htmlContent);
    sender.send(mailMessage);

    logIssueEvent(issue);
    postHandle();
  }

  private void fillContextVariable(Context context, Issue issue, OUser user) {
    context.setVariable("link", config.endpoint + "/#issues/" + issue.getIid());
    String body = "@" + user.getName() + " [" + issue.getClient().getName() + "] escalated issue #" + issue.getIid();
    context.setVariable("body", body);
  }
}
