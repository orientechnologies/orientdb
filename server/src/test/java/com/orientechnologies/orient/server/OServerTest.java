package com.orientechnologies.orient.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import java.io.File;
import java.util.ArrayList;
import java.util.logging.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 21/01/2016. */
public class OServerTest {

  private String prevPassword;
  private String prevOrientHome;
  private boolean allowJvmShutdownPrev;
  private OServer server;
  private OServerConfiguration conf;

  @Before
  public void setUp() throws Exception {
    OLogManager.instance().setConsoleLevel(Level.OFF.getName());
    prevPassword = System.setProperty("ORIENTDB_ROOT_PASSWORD", "rootPassword");
    prevOrientHome = System.setProperty("ORIENTDB_HOME", "./target/testhome");

    allowJvmShutdownPrev = OGlobalConfiguration.ENVIRONMENT_ALLOW_JVM_SHUTDOWN.getValueAsBoolean();
    OGlobalConfiguration.ENVIRONMENT_ALLOW_JVM_SHUTDOWN.setValue(false);

    conf = new OServerConfiguration();

    conf.handlers = new ArrayList<OServerHandlerConfiguration>();
    OServerHandlerConfiguration handlerConfiguration = new OServerHandlerConfiguration();
    handlerConfiguration.clazz = OServerFailingOnStarupPluginStub.class.getName();
    handlerConfiguration.parameters = new OServerParameterConfiguration[0];

    conf.handlers.add(0, handlerConfiguration);
  }

  @After
  public void tearDown() throws Exception {
    if (server.isActive()) server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File("./target/testhome"));
    // invariants
    OGlobalConfiguration.ENVIRONMENT_ALLOW_JVM_SHUTDOWN.setValue(allowJvmShutdownPrev);

    if (prevOrientHome != null) System.setProperty("ORIENTDB_HOME", prevOrientHome);
    if (prevPassword != null) System.setProperty("ORIENTDB_ROOT_PASSWORD", prevPassword);

    Orient.instance().startup();
  }

  @Test
  public void shouldShutdownOnPluginStartupException() {

    try {
      server = new OServer(false);
      server.startup(conf);
      server.activate();
    } catch (Exception e) {
      assertThat(e).isInstanceOf(OException.class);
    }

    assertThat(server.isActive()).isFalse();
    server.shutdown();
  }
}
