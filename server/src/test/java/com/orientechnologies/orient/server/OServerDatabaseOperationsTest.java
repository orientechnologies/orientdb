package com.orientechnologies.orient.server;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.logging.Level;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OServerDatabaseOperationsTest {
  private static final String SERVER_DIRECTORY = "./target/db";

  private OServer server;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException, NotCompliantMBeanException,
      MBeanRegistrationException, NoSuchMethodException, IOException, InvocationTargetException, IllegalAccessException,
      InstantiationException {
    OLogManager.instance().setConsoleLevel(Level.OFF.getName());
    OServerConfiguration conf = new OServerConfiguration();

    conf.handlers = new ArrayList<OServerHandlerConfiguration>();
    OServerUserConfiguration rootUser = new OServerUserConfiguration();
    rootUser.name = "root";
    rootUser.password = "root";
    rootUser.resources = "list";
    conf.users = new OServerUserConfiguration[] { rootUser };
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(conf);
    server.activate();
    ODocument securityConfig = new ODocument();
    securityConfig.fromJSON(OIOUtils.readStreamAsString(this.getClass().getClassLoader().getResourceAsStream("security.json")),"noMap");
    server.getSecurity().reload(securityConfig);
  }

  @After
  public void after() {
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }

  @Test
  public void testServerLoginDatabase() {
    server.serverLogin("root", "root", "list");
  }

  @Test
  public void testCreateOpenDatabase() {
    server.createDatabase("test", ODatabaseType.MEMORY, OrientDBConfig.defaultConfig());
    assertTrue(server.existsDatabase("test"));
    ODatabaseSession session = server.openDatabase("test");
    assertNotNull(session);
    session.close();
  }

}
