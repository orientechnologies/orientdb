package com.orientechnologies.website.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:fs-${spring.profiles.active}.properties")
public class FSConfiguration {

  @Value("${fs.protocol}")
  public String protocol;
  @Value("${fs.host}")
  public String host;
  @Value("${fs.port}")
  public int    port;

  @Value("${fs.username}")
  public String username;
  @Value("${fs.password}")
  public String password;

  @Value("${fs.basePath}")
  public String basePath;
}
