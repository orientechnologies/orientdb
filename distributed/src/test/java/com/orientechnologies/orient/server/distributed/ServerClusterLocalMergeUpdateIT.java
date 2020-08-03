package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Test;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/9/2015
 */
public class ServerClusterLocalMergeUpdateIT extends AbstractServerClusterMergeUpdateTest {
  @Test
  public void test() throws Exception {
    init(2);
    prepare(false);
    execute();
  }

  @Override
  protected String getDatabaseURL(ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }
}
