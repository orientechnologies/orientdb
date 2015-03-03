package com.orientechnologies.website.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import java.util.Properties;

@Configuration
@PropertySource("classpath:mail-${spring.profiles.active}.properties")
public class MailConfiguration {

  @Value("${mail.protocol}")
  protected String  protocol;
  @Value("${mail.host}")
  protected String  host;
  @Value("${mail.port}")
  protected int     port;
  @Value("${mail.smtp.port}")
  protected int     port1;
  @Value("${mail.smtp.host}")
  protected String  host1;
  @Value("${mail.smtp.auth}")
  protected boolean auth;
  @Value("${mail.smtp.starttls.enable}")
  protected boolean starttls;
  @Value("${mail.from}")
  protected String  from;
  @Value("${mail.username}")
  protected String  username;
  @Value("${mail.password}")
  protected String  password;
  @Value("${mock}")
  private boolean   mock;

  @Bean
  public JavaMailSender javaMailSender() {
    JavaMailSenderImpl mailSender = new MockMailSender(this, mock);

    Properties mailProperties = new Properties();

    mailProperties.put("mail.smtp.auth", "true");
    mailProperties.put("mail.smtp.host", host);
    mailProperties.put("mail.smtp.user", username);
    mailProperties.put("mail.smtp.password", password);
    mailProperties.put("mail.smtp.user", username);
    mailProperties.put("mail.smtp.password", password);
    mailProperties.put("mail.smtp.auth", auth);
    mailProperties.put("mail.smtp.starttls.enable", "true");
    mailProperties.put("mail.smtp.host", host1);
    mailProperties.put("mail.smtp.port", port1);
    mailProperties.put("mail.smtp.debug", "true");
    mailProperties.put("mail.smtp.ssl.trust", "*");

    mailSender.setJavaMailProperties(mailProperties);
    mailSender.setHost(host);
    mailSender.setPort(port);
    mailSender.setProtocol(protocol);
    mailSender.setUsername(username);
    mailSender.setPassword(password);
    return mailSender;
  }

  // =================================== email beans
  @Bean
  public ClassLoaderTemplateResolver emailTemplateResolver() {
    ClassLoaderTemplateResolver templateResolver = new ClassLoaderTemplateResolver();
    templateResolver.setPrefix("mail/");
    templateResolver.setTemplateMode("HTML5");
    templateResolver.setCharacterEncoding("UTF-8");
    templateResolver.setOrder(1);

    return templateResolver;
  }

}
