package com.orientechnologies.website.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Created by Enrico Risa on 30/12/14.
 */

@Configuration
@PropertySource("classpath:app-${spring.profiles.active}.properties")
public class AppConfig {


    @Value("${app.endpoint}")
    public String endpoint;

    @Value("${escalate.mail}")
    public String escalateMil;

    @Value("${escalate.mailcc}")
    public String escalateMilcc;
}
