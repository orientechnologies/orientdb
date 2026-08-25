package com.orientechnologies.orient.core.config;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;

public class OConfigReadWriteTest {

  @Test
  public void simpleTest() throws IOException {

    OConfig config = new OConfig();
    config.setDatabases("databases/");
    config.setBackup("backup/");
    OConfigReadWrite rw = new OConfigReadWrite();
    String value = rw.writeYMLString(config);
    OConfig read = rw.readYMLString(value);
    assertEquals(config.getBackup(), read.getBackup());
    assertEquals(config.getDatabases(), read.getDatabases());
  }
}
