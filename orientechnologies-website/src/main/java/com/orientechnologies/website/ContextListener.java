package com.orientechnologies.website;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.SchemaManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Created by Enrico Risa on 18/10/14.
 */
@Configuration
public class ContextListener {

  private OServer                    server;

  @Autowired
  private OrientDBConnectionSettings settings;

  @Autowired
  protected SchemaManager            schemaManager;

  @PostConstruct
  public void atStartup() {

    try {
      server = OServerMain.create();

      server.startup(Thread.currentThread().getContextClassLoader().getResourceAsStream("orientdb-server-config.xml"));
      server.activate();
      ODatabaseDocumentTx tx = Orient.instance().getDatabaseFactory().createDatabase("graph", settings.getUrl());

      OSiteSchema.createSchema(tx,schemaManager);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @PreDestroy
  public void atShutdown() {
    server.shutdown();
  }
}
