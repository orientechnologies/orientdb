package com.orientechnologies.website.events;

import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.CommentRepository;
import com.orientechnologies.website.repository.IssueRepository;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Component;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;
import reactor.event.Event;

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
  private IssueRepository      issueRepository;
  @Autowired
  private SpringTemplateEngine templateEngine;

  protected final Log          logger = LogFactory.getLog(getClass());

  public static String         EVENT  = "issue_commented";

  @Override
  public String event() {
    return EVENT;
  }

  @Override
  public void accept(Event<Comment> issueEvent) {

    Comment comment = issueEvent.getData();

    Issue issue = comment.getOwner();
    Context context = new Context();

    fillContextVariable(context, issue, comment);
    String htmlContent = templateEngine.process("newComment.html", context);

    OUser owner = comment.getUser();

    sendEmail(issue, htmlContent, owner,config);


    if (issue.getClient() != null) {
      sendSupportMail(sender, issue, htmlContent, false);
    }
    postHandle();
  }

  private void fillContextVariable(Context context, Issue issue, Comment comment) {
    context.setVariable("link", config.endpoint + "/#issues/" + issue.getIid());
    context.setVariable("body", comment.getBody());
    context.setVariable("author", "@" + comment.getUser().getName() + " commented issue #" + issue.getIid());
  }

}
