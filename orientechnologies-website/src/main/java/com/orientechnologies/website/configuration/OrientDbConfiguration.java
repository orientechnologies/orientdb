package com.orientechnologies.website.configuration;

import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.interceptor.OrientDbTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.annotation.PostConstruct;

/**
 * Created by Enrico Risa on 17/11/14.
 */
@Configuration
@EnableTransactionManagement
public class OrientDbConfiguration {

  @Autowired
  private OrientDbTransactionManager transactionManager;

  @Autowired
  private OrientDBFactory            factory;

  @PostConstruct
  public void init() {
    transactionManager.setGraphFactory(factory);
  }

  @Bean
  public OrientDbTransactionManager transactionManager() {
    transactionManager = new OrientDbTransactionManager();
    return transactionManager;
  }

}
