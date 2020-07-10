package com.orientechnologies.orient.fullsync;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.server.OServer;
import org.junit.Test;

public class FullSyncIT {

  @Test
  public void test() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    OServer server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    OServer server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    OServer server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    remote.create(FullSyncIT.class.getSimpleName(), ODatabaseType.PLOCAL);
    ODatabaseSession session = remote.open(FullSyncIT.class.getSimpleName(), "admin", "admin");
    session.createClass("test");
    session.close();
    remote.drop(FullSyncIT.class.getSimpleName());
    remote.close();
    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
  }
}
