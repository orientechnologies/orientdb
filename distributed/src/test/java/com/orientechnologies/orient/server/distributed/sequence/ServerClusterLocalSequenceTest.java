package com.orientechnologies.orient.server.distributed.sequence;

import com.orientechnologies.orient.server.distributed.ServerRun;
import org.junit.Test;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class ServerClusterLocalSequenceTest extends AbstractServerClusterSequenceTest {
    @Test
    public void test() throws Exception {
        init(2);
        prepare(false);
        execute();
    }

    protected String getDatabaseURL(final ServerRun server) {
        return "plocal:" + ServerRun.getDatabasePath(server.getServerId(), getDatabaseName());
    }
}