package com.orientechnologies.website.events;

import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.CommentRepository;
import com.orientechnologies.website.repository.IssueRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Component;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;
import reactor.event.Event;

import java.util.ArrayList;
import java.util.List;

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

  public static String         EVENT = "issue_commented";

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

    List<OUser> involvedActors = new ArrayList<OUser>();
    List<OUser> actorsInIssue = null;
    if (!Boolean.TRUE.equals(issue.getConfidential())) {
      actorsInIssue = issueRepository.findToNotifyActors(issue);
      involvedActors.addAll(actorsInIssue);
      involvedActors.addAll(issueRepository.findToNotifyActorsWatching(issue));
    } else {
      actorsInIssue = issueRepository.findToNotifyPrivateActors(issue);
      involvedActors.addAll(actorsInIssue);
    }
    String[] actors = getActorsEmail(owner, involvedActors, actorsInIssue);
    if (actors.length > 0) {
      for (String actor : actors) {
        SimpleMailMessage mailMessage = new SimpleMailMessage();
        try {
          mailMessage.setTo(actor);
          mailMessage.setFrom(comment.getUser().getName());
          mailMessage.setSubject(fillSubjectTags(issue));
          mailMessage.setText(htmlContent);
          sender.send(mailMessage);
        } catch (Exception e) {

        }
      }
    }
    if (issue.getClient() != null)
      sendSupportMail(sender, issue, htmlContent, false);
  }

  private void fillContextVariable(Context context, Issue issue, Comment comment) {
    context.setVariable("link", config.endpoint + "/#issues/" + issue.getIid());
    context.setVariable("body", comment.getBody());
    context.setVariable("author", "@" + comment.getUser().getName() + " commented issue #" + issue.getIid());
  }

}
