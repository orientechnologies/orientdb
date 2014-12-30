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
        LOGGER.info(simpleMessage.getText());
    }


    @Override
    public void send(MimeMessage mimeMessage) throws MailException {

    }
}
