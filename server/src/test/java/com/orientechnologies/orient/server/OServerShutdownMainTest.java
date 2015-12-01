package com.orientechnologies.orient.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkProtocolConfiguration;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 19/11/2015.
 */
public class OServerShutdownMainTest {

  private OServer server;

  @BeforeMethod
  public void startupOserver() throws Exception {
    OLogManager.instance().setConsoleLevel(Level.OFF.getName());
    System.setProperty("ORIENTDB_ROOT_PASSWORD", "rootPassword");
    System.setProperty("ORIENTDB_HOME", "./target/testhome");

    OGlobalConfiguration.ENVIRONMENT_ALLOW_JVM_SHUTDOWN.setValue(false);
    OServerConfiguration conf = new OServerConfiguration();
    conf.network = new OServerNetworkConfiguration();

    conf.network.protocols = new ArrayList<OServerNetworkProtocolConfiguration>();
    conf.network.protocols.add(new OServerNetworkProtocolConfiguration("binary", ONetworkProtocolBinary.class.getName()));
    conf.network.protocols.add(new OServerNetworkProtocolConfiguration("http", ONetworkProtocolHttpDb.class.getName()));

    conf.network.listeners = new ArrayList<OServerNetworkListenerConfiguration>();
    conf.network.listeners.add(new OServerNetworkListenerConfiguration());

    server = new OServer(false);
    server.startup(conf);
    server.activate();

    assertThat(server.isActive()).isTrue();

  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (server.isActive())
      server.shutdown();

  }

  @Test(enabled = true)
  public void shouldShutdowServerWithDirectCall() throws Exception {

    OServerShutdownMain shutdownMain = new OServerShutdownMain("localhost", "2424", "root", "rootPassword");
    shutdownMain.connect(5000);

    TimeUnit.SECONDS.sleep(2);
    assertThat(server.isActive()).isFalse();

  }

  @Test(enabled = true)
  public void shouldShutdowServerParsingShortArguments() throws Exception {

    OServerShutdownMain.main(new String[] { "-h", "localhost", "-P", "2424", "-p", "rootPassword", "-u", "root" });

    TimeUnit.SECONDS.sleep(2);
    assertThat(server.isActive()).isFalse();

  }

  @Test(enabled = true)
  public void shouldShutdowServerParsingLongArguments() throws Exception {

    OServerShutdownMain
        .main(new String[] { "--host", "localhost", "--ports", "2424", "--password", "rootPassword", "--user", "root" });

    TimeUnit.SECONDS.sleep(2);
    assertThat(server.isActive()).isFalse();

  }
}