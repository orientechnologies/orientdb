package com.orientechnologies.website.events;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.IssueRepository;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.reactor.ReactorMSG;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import reactor.event.Event;
import reactor.function.Consumer;

import java.util.*;

/**
 * Created by Enrico Risa on 30/12/14.
 */
public abstract class EventInternal<T> implements Consumer<Event<T>> {

  @Autowired
  @Lazy
  protected JavaMailSenderImpl     sender;

  protected final Log              logger = LogFactory.getLog(getClass());
  @Autowired
  protected OrganizationRepository organizationRepository;

  @Autowired
  private IssueRepository          issueRepository;

  @Autowired
  OrientDBFactory                  factory;

  public String handleWhat() {
    return ReactorMSG.INTERNAL_EVENT.toString() + "/" + event();
  }

  public abstract String event();

  protected String[] getActorsEmail(OUser owner, List<OUser> involvedActors, List<OUser> actorsInIssues) {
    Set<String> actors = new HashSet<String>();
    for (OUser involvedActor : involvedActors) {
      if (Boolean.TRUE.equals(involvedActor.getWatching())) {
        addEmail(owner, actors, involvedActor);
      }
    }
    for (OUser actorsInIssue : actorsInIssues) {
      if (Boolean.TRUE.equals(actorsInIssue.getNotification())) {
        addEmail(owner, actors, actorsInIssue);
      }
    }
    return actors.toArray(new String[actors.size()]);
  }

  private void addEmail(OUser owner, Set<String> actors, OUser actorsInIssue) {
    if (actorsInIssue.getWorkingEmail() != null && !actorsInIssue.getWorkingEmail().isEmpty()
        && !actorsInIssue.getWorkingEmail().equals(owner.getWorkingEmail())) {
      actors.add(actorsInIssue.getWorkingEmail());
    } else if (actorsInIssue.getEmail() != null && !actorsInIssue.getEmail().isEmpty()
        && !actorsInIssue.getEmail().equals(owner.getEmail())) {
      actors.add(actorsInIssue.getEmail());
    }
  }

  protected void sendSupportMail(JavaMailSenderImpl sender, Issue issue, String content, boolean isNew) {

    if (issue.getClient().isSupported()) {
      for (Client client : organizationRepository.findClients(issue.getRepository().getOrganization().getName())) {

        if (client.isSupport()) {

          SimpleMailMessage mailMessage = new SimpleMailMessage();
          mailMessage.setTo(client.getSupportEmail());
          mailMessage.setFrom("PrjHub");
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

  protected void postHandle() {
    OrientGraph graph = factory.getGraph();
    graph.shutdown();
    factory.unsetDb();
  }

  protected void logIssueEvent(Issue issue) {
    logger.info("Fired event : " + event() + " for issue :" + issue.getIid());

  }

  private String titleTag(Issue issue) {
    return issue.getTitle();
  }

  protected void sendEmail(Issue issue, String htmlContent, OUser user) {

    logIssueEvent(issue);

    Set<String> dests = new HashSet<String>();
    List<OUser> members = new ArrayList<>();

    List<OUser> issueActors = null;
    if (!Boolean.TRUE.equals(issue.getConfidential())) {
      issueActors = issueRepository.findToNotifyActors(issue);
      members.addAll(issueActors);
      members.addAll(issueRepository.findToNotifyActorsWatching(issue));

    } else {
      issueActors = issueRepository.findToNotifyPrivateActors(issue);
      members.addAll(issueActors);
    }
    String[] actorsEmail = getActorsEmail(user, members, issueActors);
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
          e.printStackTrace();
          logger.error(this, e);
        }
      }
    }

    OrientGraph graph = factory.getGraph();
    ODocument doc = new ODocument("MailLog");

    doc.field("issue", issue.getIid());
    doc.field("action", event());
    doc.field("actors", dests);
    doc.field("content", htmlContent);

    graph.getRawGraph().save(doc);

  }
}
