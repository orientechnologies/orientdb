package com.orientechnologies.website.events;

import com.orientechnologies.website.configuration.AppConfig;
import com.orientechnologies.website.model.schema.dto.Issue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Component;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;
import reactor.event.Event;

/**
 * Created by Enrico Risa on 30/12/14.
 */
@Component
public class IssueCreatedEvent extends EventInternal<Issue> {

    @Autowired
    @Lazy
    protected JavaMailSenderImpl sender;

    @Autowired
    protected AppConfig config;

    @Autowired
    private SpringTemplateEngine templateEngine;

    public static String EVENT = "issue_created";

    @Override
    public String event() {
        return EVENT;
    }

    @Override
    public void accept(Event<Issue> issueEvent) {

        Issue issue = issueEvent.getData();
        Context context = new Context();
        context.setVariable("link", config.endpoint + "/#issues/" + issue.getIid());
        String htmlContent = templateEngine.process("newIssue.html", context);
        SimpleMailMessage mailMessage = new SimpleMailMessage();
        mailMessage.setTo("someone@localhost");
        mailMessage.setReplyTo("someone@localhost");
        mailMessage.setFrom("someone@localhost");
        mailMessage.setSubject("Lorem ipsum");
        mailMessage.setText(htmlContent);
        sender.send(mailMessage);

    }
}
