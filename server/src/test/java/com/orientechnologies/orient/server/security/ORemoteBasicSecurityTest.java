package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertEquals;

public class ORemoteBasicSecurityTest {

  private OServer server;

  @Before
  public void before()
      throws IOException, InstantiationException, InvocationTargetException, NoSuchMethodException, MBeanRegistrationException,
      IllegalAccessException, InstanceAlreadyExistsException, NotCompliantMBeanException, ClassNotFoundException,
      MalformedObjectNameException {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = OServer.startFromClasspathConfig("abstract-orientdb-server-config.xml");

    OrientDB orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    try (ODatabaseSession session = orientDB.open("test", "admin", "admin")) {
      session.createClass("one");
      session.save(new ODocument("one"));
    }
    orientDB.close();
  }

  @Test
  public void testCreateAndConnectWriter() {
    //CREATE A SEPARATE CONTEXT TO MAKE SURE IT LOAD STAFF FROM SCRATCH
    try (OrientDB writerOrient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession writer = writerOrient.open("test", "writer", "writer")) {
        writer.save(new ODocument("one"));
        try (OResultSet rs = writer.query("select from one")) {
          assertEquals(rs.stream().count(), 2);
        }
      }
    }
  }

  @Test
  public void testCreateAndConnectReader() {
    //CREATE A SEPARATE CONTEXT TO MAKE SURE IT LOAD STAFF FROM SCRATCH
    try (OrientDB writerOrient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession writer = writerOrient.open("test", "reader", "reader")) {
        try (OResultSet rs = writer.query("select from one")) {
          assertEquals(rs.stream().count(), 1);
        }
      }
    }
  }

  @After
  public void after() {
    server.shutdown();
  }

}
