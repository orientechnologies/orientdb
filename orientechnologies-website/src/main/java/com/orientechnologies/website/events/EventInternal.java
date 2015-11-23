package com.orientechnologies.website.events;

import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.reactor.ReactorMSG;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import reactor.event.Event;
import reactor.function.Consumer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Enrico Risa on 30/12/14.
 */
public abstract class EventInternal<T> implements Consumer<Event<T>> {

  @Autowired
  protected OrganizationRepository organizationRepository;

  public String handleWhat() {
    return ReactorMSG.INTERNAL_EVENT.toString() + "/" + event();
  }

  public abstract String event();

  protected String[] getActorsEmail(OUser owner, List<OUser> involvedActors, List<OUser> actorsInIssues) {
    Set<String> actors = new HashSet<String>();
    for (OUser involvedActor : involvedActors) {
      if (Boolean.TRUE.equals(involvedActor.getWatching())) {
        if (involvedActor.getWorkingEmail() != null && !involvedActor.getWorkingEmail().isEmpty()
            && !involvedActor.getWorkingEmail().equals(owner.getWorkingEmail())) {
          actors.add(involvedActor.getWorkingEmail());
        } else if (involvedActor.getEmail() != null && !involvedActor.getEmail().isEmpty()
            && !involvedActor.getEmail().equals(owner.getEmail())) {
          actors.add(involvedActor.getEmail());
        }
      }
    }
    for (OUser actorsInIssue : actorsInIssues) {
      if (Boolean.TRUE.equals(actorsInIssue.getNotification())) {
        if (actorsInIssue.getWorkingEmail() != null && !actorsInIssue.getWorkingEmail().isEmpty()
            && !actorsInIssue.getWorkingEmail().equals(owner.getWorkingEmail())) {
          actors.add(actorsInIssue.getWorkingEmail());
        } else if (actorsInIssue.getEmail() != null && !actorsInIssue.getEmail().isEmpty()
            && !actorsInIssue.getEmail().equals(owner.getEmail())) {
          actors.add(actorsInIssue.getEmail());
        }
      }
    }
    return actors.toArray(new String[actors.size()]);
  }

  protected void sendSupportMail(JavaMailSenderImpl sender, Issue issue, String content, boolean isNew) {

    if (issue.getClient().isSupported()) {
      for (Client client : organizationRepository.findClients(issue.getRepository().getOrganization().getName())) {

        if (client.isSupport()) {

          SimpleMailMessage mailMessage = new SimpleMailMessage();
          mailMessage.setTo(client.getSupportEmail());
          mailMessage.setFrom("prjhub@orientechnologies.com");
          String subject = prepareSubject(issue, client, isNew);
          if (subject != null) {
            mailMessage.setSubject(subject);
          } else {
            mailMessage.setSubject(issue.getTitle());
          }
          mailMessage.setText(content);
          sender.send(mailMessage);
        }
      }
    }

  }

  protected String prepareSubject(Issue issue, Client support, boolean isNew) {
    String text = null;

    if (isNew) {
      text = support.getSupportSubject();
    } else {
      text = support.getSupportSubjectUpdate();
    }
    if (text != null) {
      text = text.replace("$issue", "" + issue.getIid());
      text = text.replace("$client", "" + issue.getClient().getName());
    }
    return text;
  }

  protected String fillSubjectTags(Issue issue) {
    return prjHubTag(issue) + assigneeTag(issue) + " " + titleTag(issue);
  }

  private String assigneeTag(Issue issue) {
    if (issue.getAssignee() != null) {
      return "[" + issue.getAssignee().getUsername() + "]";
    }
    return "[NotAssigned]";
  }

  private String prjHubTag(Issue issue) {
    if (issue.getClient() != null) {
      return "[PrjHub!]";
    } else {
      return "[PrjHub]";
    }
  }

  private String titleTag(Issue issue) {
    return issue.getTitle();
  }
}
