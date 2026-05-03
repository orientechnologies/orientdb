package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream.RequestNext;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.distributed.db.OutputStreamMessages;
import com.orientechnologies.orient.distributed.db.OutputStreamMessages.MessageSender;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SyncOpsTest {
  private final OLogger logger = OLogManager.instance().logger(SyncOpsTest.class);
  private OrientDB context;
  private OrientDB context1;
  private ODatabaseId dbId;

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException {
    OrientDBConfigBuilder config1 = OrientDBConfig.builder();
    config1
        .getNodeConfigurationBuilder()
        .setNodeName("node1")
        .setGroupName("OrientDB")
        .setQuorum(1);
    context = OrientDBInternal.distributed("./target/sync", config1.build()).newOrientDB();
    context.execute("create database test plocal users(admin identified by 'adminpwd' role admin)");
    OrientDBDistributed ctx = (OrientDBDistributed) OrientDBInternal.extract(context);
    dbId = ctx.getStorage("test").getDatabaseId();
    OrientDBConfigBuilder config2 = OrientDBConfig.builder();
    config2
        .getNodeConfigurationBuilder()
        .setNodeName("node2")
        .setGroupName("OrientDB")
        .setQuorum(1);
    context1 = OrientDBInternal.distributed("./target/sync_receive", config2.build()).newOrientDB();
    OrientDBDistributed ctx1 = (OrientDBDistributed) OrientDBInternal.extract(context1);
    ctx1.declareDatabaseFlow("test", dbId).get(1, TimeUnit.MINUTES);
  }

  private class PassTrough implements RequestNext, MessageSender {
    private OSyncState sender;
    protected OSyncState receiver;

    public PassTrough(OSyncState sender, OSyncState receiver) {
      this.sender = sender;
      this.receiver = receiver;
    }

    @Override
    public void requestNext(OSyncState state, boolean close) {
      logger.debug("requesting next from receiver close:%b", close);
      this.sender.requestNext(close);
    }

    @Override
    public void sendBuffer(OSyncState state, byte[] data, long sequential, boolean finished) {
      logger.debug("sending data from sender size:%s finished:%b", data.length, finished);

      this.receiver.receiveData(data, sequential, finished);
      state.transaferd(data.length);
      if (!finished) {
        try {
          state.waitForNext();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private class FailPassTrough extends PassTrough {
    private int failCount;

    public FailPassTrough(OSyncState sender, OSyncState receiver, int failCount) {
      super(sender, receiver);
      this.failCount = failCount;
    }

    @Override
    public void sendBuffer(OSyncState state, byte[] data, long sequential, boolean finished) {
      if (this.failCount > 0) {
        super.sendBuffer(state, data, sequential, finished);
      } else {
        this.receiver.close();
      }
      this.failCount -= 1;
    }
  }

  private void testRawSync(OSyncMode mode) {
    var nodeFrom = new ONodeId("node1");
    var nodeTo = new ONodeId("node2");
    var syncId = new OSyncId(dbId, nodeTo);

    var sender =
        new OSyncState(dbId, syncId, nodeFrom, nodeTo, mode, Optional.empty(), (res) -> {});
    var receiver =
        new OSyncState(dbId, syncId, nodeFrom, nodeTo, mode, Optional.empty(), (res) -> {});
    var pass = new PassTrough(sender, receiver);

    OutputStream out = new OutputStreamMessages(pass, sender);
    OReceiverInputStream input = new OReceiverInputStream(pass, receiver);
    receiver.setReceiverStream(input);

    OrientDBDistributed ctx = (OrientDBDistributed) OrientDBInternal.extract(context);
    var senderStatus = ctx.getDatabase("test").status();
    new Thread(
            () -> {
              try {
                ctx.syncBackup("test", sender, out);
              } catch (Exception e) {
                e.printStackTrace();
              }
            })
        .start();

    OrientDBDistributed ctx1 = (OrientDBDistributed) OrientDBInternal.extract(context1);
    var result = ctx1.receiveSync("test", receiver, input, OrientDBConfig.defaultConfig());
    assertTrue(result);
    try (var session = context1.open("test", "admin", "adminpwd")) {
      // if it can open is good, it restored the right password
      assertEquals(senderStatus, ctx1.getDatabase("test").status());
      assertTrue(true);
    }
  }

  private void testRawSyncTwice(OSyncMode mode) {
    testRawSync(mode);
    testRawSync(mode);
  }

  @Test
  public void testRawSyncTwiceBackup() {
    testRawSyncTwice(OSyncMode.StandardBackup);
  }

  @Test
  public void testRawSyncTwiceIncremental() {
    testRawSyncTwice(OSyncMode.IncrementalBackup);
  }

  @Test
  public void testRawSyncBackup() {
    testRawSync(OSyncMode.StandardBackup);
  }

  @Test
  public void testRawSyncIncremental() {
    testRawSync(OSyncMode.IncrementalBackup);
  }

  @Test
  public void testFailRawSyncIncremental() {
    testFailRawSync(OSyncMode.IncrementalBackup);
  }

  @Test
  public void testFailRawSyncBackup() {
    testFailRawSync(OSyncMode.StandardBackup);
  }

  public void testFailRawSync(OSyncMode mode) {
    var nodeFrom = new ONodeId("node1");
    var nodeTo = new ONodeId("node2");
    var syncId = new OSyncId(dbId, nodeTo);

    var sender =
        new OSyncState(dbId, syncId, nodeFrom, nodeTo, mode, Optional.empty(), (res) -> {});
    var receiver =
        new OSyncState(dbId, syncId, nodeFrom, nodeTo, mode, Optional.empty(), (res) -> {});
    var pass = new FailPassTrough(sender, receiver, 5);

    OutputStream out = new OutputStreamMessages(pass, sender);
    OReceiverInputStream input = new OReceiverInputStream(pass, receiver);
    receiver.setReceiverStream(input);

    OrientDBDistributed ctx = (OrientDBDistributed) OrientDBInternal.extract(context);
    new Thread(
            () -> {
              try {
                ctx.syncBackup("test", sender, out);
              } catch (Exception e) {
                e.printStackTrace();
              }
            })
        .start();

    OrientDBDistributed ctx1 = (OrientDBDistributed) OrientDBInternal.extract(context1);
    var result = ctx1.receiveSync("test", receiver, input, OrientDBConfig.defaultConfig());
    assertFalse(result);
    assertFalse(ctx1.exists("test", null, null));
    try {
      // if it can open is good, it restored the right password
      if (ctx1.getNodeState().getOps().waitSelfOnline("test", Optional.of(1L))) {
        fail("Should not open not synched");
      }
    } catch (InterruptedException e) {
    }
  }

  @After
  public void after() {
    context.drop("test");
    context.close();
    if (context1.exists("test")) {
      context1.drop("test");
    }
    context1.close();
  }
}
