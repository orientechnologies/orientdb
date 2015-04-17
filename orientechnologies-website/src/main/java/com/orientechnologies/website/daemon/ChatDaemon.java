package com.orientechnologies.website.daemon;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.ClientRepository;
import com.orientechnologies.website.repository.MessageRepository;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.OrganizationService;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Enrico Risa on 15/04/15.
 */
@Component
public class ChatDaemon {

  @Autowired
  @Lazy
  protected JavaMailSenderImpl     sender;
  @Autowired
  private SpringTemplateEngine     templateEngine;

  @Autowired
  private OrientDBFactory          dbFactory;
  @Autowired
  protected OrganizationService    organizationService;

  @Autowired
  protected MessageRepository      messageRepository;

  @Autowired
  protected AppConfig              config;

  @Autowired
  protected OrganizationRepository organizationRepository;

  @Autowired
  protected ClientRepository       clientRepository;

  @Scheduled(fixedDelay = 10 * 60 * 1000)
  public void sendMessageNotification() {

    Iterable<Organization> organizations = organizationRepository.findAll();
    for (Organization organization : organizations) {

      OrientGraph db = dbFactory.getGraph();
      Iterable<Client> clients = clientRepository.findAll();
      for (Client client : clients) {

        OClass oClass = db.getRawGraph().getMetadata().getSchema().getClass("Chat" + client.getClientId());
        if (oClass != null) {
          List<OUser> actors = organizationService.getClientRoomActors(organization.getName(), client.getClientId());

          Map<Date, OUser> activities = new ConcurrentHashMap<Date, OUser>();

          for (OUser actor : actors) {
            Date lastActivity = messageRepository.findLastActivityIfNotNotified(actor, client);
            if (lastActivity != null) {
              activities.put(lastActivity, actor);
            }
          }
          ORecordIteratorCluster<ODocument> iterator = new ORecordIteratorCluster(db.getRawGraph(), db.getRawGraph(),
              oClass.getDefaultClusterId(), 0, ORID.CLUSTER_POS_INVALID, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
          iterator.last();
          while (iterator.hasPrevious() && activities.size() > 0) {
            ODocument doc = iterator.previous();
            Date date = doc.field("date");
            for (Date date1 : activities.keySet()) {
              if (date1.after(date)) {
                activities.remove(date1);
              } else {
                sendMailAndNotify(db, client, activities, date1);
              }
            }
          }
        }

      }

      break;
    }
  }

  private void sendMailAndNotify(OrientGraph graph, final Client client, Map<Date, OUser> activities, Date date1) {
    Context context = new Context();

    final OUser user = activities.get(date1);

    if (Boolean.TRUE.equals(user.getChatNotification())) {
      context.setVariable("link", config.endpoint + "/#rooms/" + client.getClientId());
      context.setVariable("linkSettings", config.endpoint + "#/users/" + user.getUsername());
      context.setVariable("user", "Hi @" + user.getUsername());
      context.setVariable("room", "You have unread message in room: <b>" + client.getName() + "</b>");
      String htmlContent = templateEngine.process("unread.html", context);
      SimpleMailMessage mailMessage = new SimpleMailMessage();
      String email = user.getWorkingEmail() != null ? user.getWorkingEmail() : user.getEmail();
      mailMessage.setTo(email);
      mailMessage.setFrom("prjhub@orientechnologies.com");
      mailMessage.setSubject("Unread message from room: " + client.getName());
      mailMessage.setText(htmlContent);
      Map<String, Object> params = new HashMap<String, Object>() {
        {
          put("user", user.getRid());
          put("room", client.getId());
        }
      };
      graph.command(
          new OCommandSQL("update ChatLog SET user=:user, room=:room , notified=true UPSERT WHERE user=:user and room =:room"))
          .execute(params);

      graph.commit();
      sender.send(mailMessage);
      activities.remove(date1);

    }
  }
}
