package com.orientechnologies.orient.core;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Test;

/** Created by tglman on 21/02/17. */
public class OGlobalConfigurationTest {

  @Test
  public void testReadFromEnv() {

    Assert.assertEquals(
        "ORIENTDB_DISTRIBUTED", OGlobalConfiguration.getEnvKey(OGlobalConfiguration.DISTRIBUTED));

    Assert.assertEquals(
        "ORIENTDB_DISTRIBUTED_NODE_NAME",
        OGlobalConfiguration.getEnvKey(OGlobalConfiguration.DISTRIBUTED_NODE_NAME));
  }

  /** OGlobalConfiguration.DISTRIBUTED has no explicit "section" */
  @Test
  public void testDumpConfiguraiton() {
    final OutputStream os = new ByteArrayOutputStream();
    OGlobalConfiguration.dumpConfiguration(new PrintStream(os));
    Assert.assertTrue(os.toString().contains(OGlobalConfiguration.DISTRIBUTED.getKey()));
  }
}
