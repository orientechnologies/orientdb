package com.orientechnologies.orient.server.config;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OConfigReadWrite;
import java.io.File;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.Test;
import org.xml.sax.SAXException;

public class OConfigRewriteFromServerConfigTest {

  @Test
  public void testBase() throws IOException, ParserConfigurationException, SAXException {
    OServerConfigurationRewrite rewriter = new OServerConfigurationRewrite();
    var mg = new OServerConfigurationManager(new File("./src/test/fixtures/example_one.xml"));
    var config = rewriter.convertToOConfig(mg.getConfiguration());

    var crw = new OConfigReadWrite();
    var cfg = crw.writeYMLString(config);
    var read = crw.readYMLString(cfg);
    assertEquals(read.getDatabases(), "databases");
    assertEquals(read.getListeners().size(), 2);
    assertEquals(read.getListeners().get(0).getProtocol(), "binary");
    assertEquals(read.getListeners().get(0).getHost(), "0.0.0.0");
    assertEquals(read.getListeners().get(0).getPortRange(), "2426-2430");

    assertEquals(read.getSecurity().getUsers().size(), 2);
    assertEquals(read.getSecurity().getUsers().get(0).getUser(), "root");
    assertEquals(read.getSecurity().getUsers().get(0).getPassword(), "root");
  }

  @Test
  public void testDistributed() throws IOException, ParserConfigurationException, SAXException {
    OServerConfigurationRewrite rewriter = new OServerConfigurationRewrite();
    var mg =
        new OServerConfigurationManager(
            new File("./src/test/fixtures/distributed/distributed.xml"));
    var config = rewriter.convertToOConfig(mg.getConfiguration());

    var crw = new OConfigReadWrite();
    var cfg = crw.writeYMLString(config);
    var read = crw.readYMLString(cfg);
    var cfg1 = crw.writeYMLString(config);
    assertEquals(read.getDatabases(), "databases");
    assertEquals(read.getListeners().size(), 2);
    assertEquals(read.getListeners().get(0).getProtocol(), "binary");
    assertEquals(read.getListeners().get(0).getHost(), "0.0.0.0");
    assertEquals(read.getListeners().get(0).getPortRange(), "2424-2430");

    assertEquals(read.getSecurity().getUsers().size(), 2);
    assertEquals(read.getSecurity().getUsers().get(0).getUser(), "root");
    assertEquals(read.getSecurity().getUsers().get(0).getPassword(), "test");

    assertEquals(read.getDistributed().isEnabled(), true);
    assertEquals(read.getDistributed().getNode(), "europe");
    assertEquals(read.getDistributed().getNetworkMinimumQuorum(), 2);
    assertEquals(read.getDistributed().getGroup(), "orientdb");
    assertEquals(read.getDistributed().getKey(), "orientdb");
  }
}
