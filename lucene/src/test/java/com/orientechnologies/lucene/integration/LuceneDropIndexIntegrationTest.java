package com.orientechnologies.lucene.integration;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LuceneDropIndexIntegrationTest {
  private OServer server0;
  private OServer server1;

  @Before
  public void before() throws Exception {
    server0 =
        OServer.startFromClasspathConfig(
            "com/orientechnologies/lucene/integration/orientdb-simple-dserver-config-0.xml");
    server1 =
        OServer.startFromClasspathConfig(
            "com/orientechnologies/lucene/integration/orientdb-simple-dserver-config-1.xml");
    final OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database LuceneDropIndexIntegrationTest plocal users(admin identified by 'admin' role admin)");
    ODatabaseSession session = remote.open("LuceneDropIndexIntegrationTest", "admin", "admin");

    session.command("create class Person");
    session.command("create property Person.name STRING");
    session.command("create property Person.surname STRING");

    session.command("create index Person.name on Person(name) FULLTEXT ENGINE LUCENE");
    session.command("create index Person.surname on Person(surname) FULLTEXT ENGINE LUCENE");

    OElement doc = session.newElement("Person");
    doc.setProperty("name", "Jon");
    doc.setProperty("surname", "Snow");
    session.save(doc);
    session.close();
    remote.close();
  }

  @Test
  public void testDropDatabase() throws IOException {

    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop("LuceneDropIndexIntegrationTest");
    remote.close();

    List<Path> paths =
        Files.walk(Paths.get(server0.getDatabaseDirectory()))
            .filter(f -> f.toFile().getName().equalsIgnoreCase("LuceneDropIndexIntegrationTest"))
            .collect(Collectors.toList());

    Assert.assertEquals(0, paths.size());

    paths =
        Files.walk(Paths.get(server1.getDatabaseDirectory()))
            .filter(f -> f.toFile().getName().equalsIgnoreCase("LuceneDropIndexIntegrationTest"))
            .collect(Collectors.toList());

    Assert.assertEquals(0, paths.size());
  }

  @After
  public void after()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException,
          NoSuchMethodException, IOException, InvocationTargetException {

    server0.shutdown();
    server1.shutdown();
  }
}
