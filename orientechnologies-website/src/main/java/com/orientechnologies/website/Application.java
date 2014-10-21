package com.orientechnologies.website;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.website.interceptor.OrientDBFactoryInterceptor;
import com.orientechnologies.website.model.schema.OSiteSchema;

@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = "com.orientechnologies.website")
@EnableWebMvc
public class Application extends WebMvcConfigurerAdapter {

  @Autowired
  private OrientDBFactoryInterceptor interceptor;

  public static void main(final String[] args) throws Exception {

    SpringApplication.run(Application.class, args);

  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(interceptor);
  }
}
