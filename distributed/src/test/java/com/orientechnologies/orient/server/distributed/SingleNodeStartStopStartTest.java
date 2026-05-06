package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SingleNodeStartStopStartTest {

  @Before
  public void before() {}

  @After
  public void after() {}

  @Test
  public void nodeRestartTest()
      throws ClassNotFoundException,
          InstantiationException,
          IllegalAccessException,
          IOException,
          InterruptedException {
    String workingDir = null;
    try {
      OServer server = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
      workingDir = server.getDatabaseDirectory();
      OrientDB ctx = server.getContext();
      ctx.execute("create database test plocal ").close();
      ctx.execute("create database test1 plocal ").close();

      OrientDBDistributed distContext = (OrientDBDistributed) server.getDatabases();
      distContext.waitOnline("test");
      distContext.waitOnline("test1");

      assertTrue(distContext.isDatabaseOnline("test"));
      assertTrue(distContext.isDatabaseOnline("test1"));
      server.shutdown();
      OServer server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");

      OrientDBDistributed distContext1 = (OrientDBDistributed) server1.getDatabases();
      distContext1.waitOnline("test");
      distContext1.waitOnline("test1");

      assertTrue(distContext1.isDatabaseOnline("test"));
      assertTrue(distContext1.isDatabaseOnline("test1"));
      server1.shutdown();
    } finally {
      OFileUtils.deleteRecursively(new File(workingDir));
    }
  }
}
