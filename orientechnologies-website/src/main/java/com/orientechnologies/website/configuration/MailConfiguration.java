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
    private String protocol;
    @Value("${mail.host}")
    private String host;
    @Value("${mail.port}")
    private int port;
    @Value("${mail.smtp.port}")
    private int port1;
    @Value("${mail.smtp.host}")
    private String host1;
    @Value("${mail.smtp.auth}")
    private boolean auth;
    @Value("${mail.smtp.starttls.enable}")
    private boolean starttls;
    @Value("${mail.from}")
    private String from;
    @Value("${mail.username}")
    private String username;
    @Value("${mail.password}")
    private String password;
    @Value("${mock}")
    private boolean mock;

    @Bean
    public JavaMailSender javaMailSender() {
        JavaMailSenderImpl mailSender = null;
        if (mock) {
            mailSender = new MockMailSender();
        } else {
            mailSender = new JavaMailSenderImpl();
        }
        Properties mailProperties = new Properties();
        mailProperties.put("mail.smtp.auth", auth);
        mailProperties.put("mail.smtp.starttls.enable", starttls);
        mailProperties.put("mail.smtp.host",host1);
        mailProperties.put("mail.smtp.port",port1);
        mailProperties.put("mail.smtp.debug", "true");
        mailSender.setJavaMailProperties(mailProperties);
        mailSender.setHost(host);
        mailSender.setPort(port);
        mailSender.setProtocol(protocol);
        mailSender.setUsername(username);
        mailSender.setPassword(password);
        return mailSender;
    }

    //=================================== email beans
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
