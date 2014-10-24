package com.orientechnologies.website;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
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
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;

import java.util.concurrent.CountDownLatch;

import static reactor.event.selector.Selectors.$;

@Configuration
@EnableWebMvc
@EnableAutoConfiguration
@ComponentScan(basePackages = "com.orientechnologies.website")
public class Application extends WebMvcConfigurerAdapter {

  public static void main(final String[] args) throws Exception {

    SpringApplication.run(Application.class, args);

  }

}
