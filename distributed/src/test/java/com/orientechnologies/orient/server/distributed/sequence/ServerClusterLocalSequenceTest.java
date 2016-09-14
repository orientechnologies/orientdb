package com.orientechnologies.orient.server.distributed.sequence;

import org.junit.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.distributed.ServerRun;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class ServerClusterLocalSequenceTest extends AbstractServerClusterSequenceTest {
  @Test
  public void test() throws Exception {

    final long previous = OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsLong();
    try {
      OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.setValue(0);

      init(2);
      prepare(false);
      execute();

    } finally {
      OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.setValue(previous);

    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + ServerRun.getDatabasePath(server.getServerId(), getDatabaseName());
  }
}