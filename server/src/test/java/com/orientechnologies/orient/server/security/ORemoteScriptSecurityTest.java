package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ORemoteScriptSecurityTest {

  private OServer server;

  @Before
  public void before()
      throws IOException, InstantiationException, InvocationTargetException, NoSuchMethodException,
          MBeanRegistrationException, IllegalAccessException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, ClassNotFoundException, MalformedObjectNameException {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = OServer.startFromClasspathConfig("abstract-orientdb-server-config.xml");

    OrientDB orientDB =
        new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ORemoteScriptSecurityTest memory users (admin identified by 'admin' role admin)");

    orientDB.close();
  }

  @Test(expected = OSecurityException.class)
  public void testRunJavascript() {
    // CREATE A SEPARATE CONTEXT TO MAKE SURE IT LOAD STAFF FROM SCRATCH
    try (OrientDB writerOrient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession writer =
          writerOrient.open("ORemoteScriptSecurityTest", "reader", "reader")) {
        try (OResultSet rs = writer.execute("javascript", "1+1;")) {}
      }
    }
  }

  @Test(expected = OSecurityException.class)
  public void testRunEcmascript() {
    // CREATE A SEPARATE CONTEXT TO MAKE SURE IT LOAD STAFF FROM SCRATCH
    try (OrientDB writerOrient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession writer =
          writerOrient.open("ORemoteScriptSecurityTest", "reader", "reader")) {

        try (OResultSet rs = writer.execute("ecmascript", "1+1;")) {}
      }
    }
  }

  @After
  public void after() {
    server.shutdown();
  }
}
