package com.orientechnologies.orient.core;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 21/02/17.
 */
public class OGlobalConfigurationTest {

  @Test
  public void testReadFromEnv() {

    Assert.assertEquals("ORIENTDB_DISTRIBUTED", OGlobalConfiguration.getEnvKey(OGlobalConfiguration.DISTRIBUTED));

    Assert
        .assertEquals("ORIENTDB_DISTRIBUTED_NODE_NAME", OGlobalConfiguration.getEnvKey(OGlobalConfiguration.DISTRIBUTED_NODE_NAME));

  }

}
