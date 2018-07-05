package com.orientechnologies.website.security;

import com.orientechnologies.website.services.TokenAuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Created by Enrico Risa on 21/10/14.
 */

@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter {

  @Autowired
  private TokenAuthenticationService tokenAuthenticationService;

  @Override
  protected void configure(HttpSecurity http) throws Exception {

    http.csrf().disable();

    http.antMatcher("/api/v1/**").authorizeRequests().antMatchers("/api/v1/users").permitAll().antMatchers("/api/v1/login")
        .permitAll().antMatchers("/api/v1/resetPassword").permitAll().anyRequest().authenticated().and()
        .addFilterBefore(new StatelessAuthenticationFilter(tokenAuthenticationService), UsernamePasswordAuthenticationFilter.class);
  }

}
