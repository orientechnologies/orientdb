package com.orientechnologies.website.events;

import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.repository.CommentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Component;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;

import reactor.event.Event;

import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.Issue;

/**
 * Created by Enrico Risa on 30/12/14.
 */
@Component
public class IssueCommentedEvent extends EventInternal<Comment> {

  @Autowired
  @Lazy
  protected JavaMailSenderImpl sender;

  @Autowired
  protected CommentRepository  commentRepository;
  @Autowired
  protected AppConfig          config;

  @Autowired
  private SpringTemplateEngine templateEngine;

  public static String         EVENT = "issue_commented";

  @Override
  public String event() {
    return EVENT;
  }

  @Override
  public void accept(Event<Comment> issueEvent) {

    Comment comment = issueEvent.getData();
    Issue issue = commentRepository.findIssueByComment(comment);
    Context context = new Context();

    fillContextVariable(context, issue, comment);
    String htmlContent = templateEngine.process("newIssue.html", context);
    SimpleMailMessage mailMessage = new SimpleMailMessage();
    mailMessage.setTo("someone@localhost");
    mailMessage.setReplyTo("someone@localhost");
    mailMessage.setFrom("someone@localhost");
    mailMessage.setSubject("Lorem ipsum");
    mailMessage.setText(htmlContent);
    sender.send(mailMessage);

  }

  private void fillContextVariable(Context context, Issue issue, Comment comment) {
    context.setVariable("link", config.endpoint + "/#issues/" + issue.getIid());
  }

}
