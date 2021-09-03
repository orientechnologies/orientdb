package com.orientechnologies.orient.server.distributed.sequence;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.distributed.ServerRun;
import org.junit.Test;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class ServerClusterLocalSequenceIT extends AbstractServerClusterSequenceTest {
  @Test
  public void test() throws Exception {

    final long previous = OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsLong();
    try {
      OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.setValue(0);
      //      OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION.setValue(2);

      init(2);
      prepare(false);
      execute();

    } finally {
      OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.setValue(previous);
    }
  }

  @Override
  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + ServerRun.getDatabasePath(server.getServerId(), getDatabaseName());
  }
}
