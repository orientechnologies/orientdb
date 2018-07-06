package com.orientechnologies.website.events;

import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.OUser;
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
public class AccountRestoreEvent extends EventInternal<Map<String, Object>> {

  @Autowired
  @Lazy
  protected JavaMailSenderImpl sender;

  @Autowired
  protected AppConfig config;

  @Autowired
  private SpringTemplateEngine templateEngine;

  public static String EVENT = "account_restore";

  @Override
  public String event() {
    return EVENT;
  }

  @Override
  public void accept(Event<Map<String, Object>> accountRestore) {

    Map<String, Object> data = accountRestore.getData();
    OUser user = (OUser) data.get("user");
    String token = (String) data.get("token");

    Context context = new Context();

    fillContextVariable(context,token,user);
    String htmlContent = templateEngine.process("restoreMail.html", context);
    SimpleMailMessage mailMessage = new SimpleMailMessage();
    mailMessage.setTo(user.getWorkingEmail());
    mailMessage.setFrom(user.getName());
    mailMessage.setSubject("Prjhub password recovery");
    mailMessage.setText(htmlContent);
    sender.send(mailMessage);

    postHandle();
  }

  private void fillContextVariable(Context context, String token, OUser user) {
    context.setVariable("link", config.endpoint + "/#/validate-token/" + token);
    String body = "A reset password event on this account `" + user.getName()
        + "` has been triggered. Click the link below to recover the password.";
    context.setVariable("body", body);
  }
}
