package com.orientechnologies.website.security;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

/**
 * Created by Enrico Risa on 21/10/14.
 */

@Configuration
@EnableWebSecurity
@Order(1)
public class EventSecurityConfig extends WebSecurityConfigurerAdapter {


  @Override
  protected void configure(HttpSecurity http) throws Exception {

    http.csrf().disable();

    http.antMatcher("/api/v1/github/**").authorizeRequests().anyRequest().permitAll();

  }

}
