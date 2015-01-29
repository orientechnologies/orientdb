package com.orientechnologies.website.configuration;

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

//    Session session = Session.getDefaultInstance(getJavaMailProperties());
//    MimeMessage message = new MimeMessage(session);
//    try {
//      message.setText(simpleMessage.getText());
//      message.setSubject(simpleMessage.getSubject());
//      InternetAddress address = new InternetAddress(simpleMessage.getTo()[0]);
//      InternetAddress from = new InternetAddress("prjhub@orientechnologies.com");
//      message.setFrom(from);
//      message.setRecipient(Message.RecipientType.TO, address);
//      Transport.send(message);
//    } catch (MessagingException e) {
//      e.printStackTrace();
//    }
    LOGGER.info(simpleMessage.getText());
  }

  @Override
  public void send(MimeMessage mimeMessage) throws MailException {

  }
}
