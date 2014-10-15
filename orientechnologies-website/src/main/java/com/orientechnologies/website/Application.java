package com.orientechnologies.website;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.website.model.schema.OSiteSchema;

@Configuration
@EnableAutoConfiguration
@ComponentScan
public class Application extends SpringBootServletInitializer {

  public static void main(final String[] args) throws Exception {

    ApplicationContext context = SpringApplication.run(Application.class, args);
    OServer server = OServerMain.create();

    server.startup(Thread.currentThread().getContextClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    ODatabaseDocumentTx tx = Orient.instance().getDatabaseFactory().createDatabase("graph", "plocal:databases/todo");

    OSiteSchema.createSchema(tx);
  }
}
