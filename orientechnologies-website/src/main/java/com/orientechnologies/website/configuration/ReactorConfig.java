package com.orientechnologies.website.configuration;

import com.orientechnologies.website.services.reactor.GitHubIssueImporter;
import com.orientechnologies.website.services.reactor.ReactorMSG;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;

import javax.annotation.PostConstruct;

import static reactor.event.selector.Selectors.$;

/**
 * Created by Enrico Risa on 24/10/14.
 */
@Configuration
public class ReactorConfig {

  @Autowired
  private GitHubIssueImporter hubIssueImporter;
  @Autowired
  private Reactor             reactor;

  @PostConstruct
  public void startup() {

    reactor.on($(ReactorMSG.ISSUE_IMPORT), hubIssueImporter);
  }

  @Bean
  Environment env() {
    return new Environment();
  }

  @Bean
  Reactor createReactor(Environment env) {
    return Reactors.reactor().env(env).dispatcher(Environment.THREAD_POOL).get();
  }
}
