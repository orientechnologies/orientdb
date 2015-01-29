package com.orientechnologies.website.events;

import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.OUser;
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
    Comment committed = commentRepository.reload(comment);
    Issue issue = commentRepository.findIssueByComment(committed);
    Context context = new Context();

    fillContextVariable(context, issue, comment);
    String htmlContent = templateEngine.process("newComment.html", context);
    SimpleMailMessage mailMessage = new SimpleMailMessage();
    OUser owner = issue.getScope().getOwner();
    mailMessage.setTo(owner.getEmail());
    mailMessage.setFrom("prjhub@orientechnologies.com");
    mailMessage.setSubject(issue.getTitle());
    mailMessage.setText(htmlContent);
    sender.send(mailMessage);

  }

  private void fillContextVariable(Context context, Issue issue, Comment comment) {
    context.setVariable("link", config.endpoint + "/#issues/" + issue.getIid());
    context.setVariable("body", comment.getBody());
  }

}
