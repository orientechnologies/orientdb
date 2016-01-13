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

        email.setSmtpPort(mailConfiguration.port);
        email.setAuthenticator(new DefaultAuthenticator(mailConfiguration.username, mailConfiguration.password));
        email.setDebug(false);

        email.setHostName(mailConfiguration.host);
        email.setFrom(mailConfiguration.from, simpleMessage.getFrom());
        email.setSubject(simpleMessage.getSubject());
        email.setContent(simpleMessage.getText(), "text/html");
        for (String s : simpleMessage.getTo()) {
          email.addTo(s);
        }
        if (simpleMessage.getCc() != null) {
          for (String s : simpleMessage.getCc()) {
            email.addCc(s);
          }
        }

        email.setTLS(mailConfiguration.starttls);
        email.send();
        email.setSSL(false);
      } catch (Exception e) {
        e.printStackTrace();
      }

    } else {
      LOGGER.info(simpleMessage.getSubject());
      LOGGER.info(simpleMessage.getText());
    }
  }

  @Override
  public void send(MimeMessage mimeMessage) throws MailException {

  }
}
