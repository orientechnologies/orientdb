package com.orientechnologies.website.configuration;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessagePreparator;

import javax.mail.internet.MimeMessage;

/**
 * Created by Enrico Risa on 30/12/14.
 */
public class MockMailSender extends JavaMailSenderImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockMailSender.class);
  private MailConfiguration   mailConfiguration;
  private Boolean             mock;

  public MockMailSender(MailConfiguration mailConfiguration, Boolean mock) {

    this.mailConfiguration = mailConfiguration;
    this.mock = mock;
  }

  @Override
  public void send(MimeMessagePreparator mimeMessagePreparator) throws MailException {
    final MimeMessage message = createMimeMessage();

    try {
      mimeMessagePreparator.prepare(message);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void send(SimpleMailMessage simpleMessage) throws MailException {

    if (!mock) {
      try {
        Email email = new HtmlEmail();

        email.setSmtpPort(587);
        email.setAuthenticator(new DefaultAuthenticator("prjhub@orientechnologies.com", "Prj_Hubb_WoW945"));
        email.setDebug(false);

        email.setHostName("mail.orientechnologies.com");
        email.setFrom("prjhub@orientechnologies.com", "PrjHub");
        email.setSubject(simpleMessage.getSubject());
        email.setContent(simpleMessage.getText(),"text/html");
        for (String s : simpleMessage.getTo()) {
          email.addTo(s);
        }
        email.setTLS(false);
        email.send();
        email.setSSL(false);
      } catch (Exception e) {
        e.printStackTrace();
      }

    } else {
      LOGGER.info(simpleMessage.getText());
    }
  }

  @Override
  public void send(MimeMessage mimeMessage) throws MailException {

  }
}
