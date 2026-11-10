package com.orientechnologies.orient.server.config;

import com.orientechnologies.orient.core.config.OConfigReadWrite;
import java.io.File;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.Test;
import org.xml.sax.SAXException;

public class OConfigRewriteFromServerConfigTest {

  @Test
  public void test() throws IOException, ParserConfigurationException, SAXException {
    OServerConfigurationRewrite rewriter = new OServerConfigurationRewrite();
    var mg = new OServerConfigurationManager(new File("./src/test/fixtures/example_one.xml"));
    var config = rewriter.convertToOConfig(mg.getConfiguration());

    var crw = new OConfigReadWrite();
    var cfg = crw.writeYMLString(config);
    System.out.println(cfg);
  }
}
