package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream.RequestNext;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.distributed.db.OutputStreamMessages;
import com.orientechnologies.orient.distributed.db.OutputStreamMessages.MessageSender;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SyncOpsTest {

  private OrientDB context;
  private OrientDB context1;

  @Before
  public void before() {
    OrientDBConfigBuilder config1 = OrientDBConfig.builder();
    config1.getNodeConfigurationBuilder().setNodeName("node1");
    context = OrientDBInternal.distributed("./target/sync", config1.build()).newOrientDB();
    context.execute("create database test plocal users(admin identified by 'adminpwd' role admin)");
    OrientDBConfigBuilder config2 = OrientDBConfig.builder();
    config2.getNodeConfigurationBuilder().setNodeName("node2");
    context1 = OrientDBInternal.distributed("./target/sync_receive", config2.build()).newOrientDB();
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
      this.sender.requestNext(close);
    }

    @Override
    public void sendBuffer(OSyncState state, byte[] data, boolean finished) {
      this.receiver.receiveData(data, finished);
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
    public void sendBuffer(OSyncState state, byte[] data, boolean finished) {
      if (this.failCount > 0) {
        super.sendBuffer(state, data, finished);
      } else {
        this.receiver.close();
      }
      this.failCount -= 1;
    }
  }

  private void testRawSync(OSyncMode mode) {
    var dbId = new ODatabaseId("test");
    var nodeFrom = new ONodeId("node1");
    var nodeTo = new ONodeId("node2");
    var syncId = new OSyncId(dbId, nodeTo);

    var sender =
        new OSyncState(
            dbId, syncId, nodeFrom, nodeTo, mode, Optional.empty(), new CompletableFuture<>());
    var receiver =
        new OSyncState(
            dbId, syncId, nodeFrom, nodeTo, mode, Optional.empty(), new CompletableFuture<>());
    var pass = new PassTrough(sender, receiver);

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
    ctx1.receiveSync("test", receiver, input, OrientDBConfig.defaultConfig());
    try (var session = context1.open("test", "admin", "adminpwd")) {
      // if it can open is good, it restored the right password
      assertTrue(true);
    }
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
    var syncId = new OSyncId();
    var dbId = new ODatabaseId("test");
    var nodeFrom = new ONodeId("node1");
    var nodeTo = new ONodeId("node2");

    var sender =
        new OSyncState(
            dbId, syncId, nodeFrom, nodeTo, OSyncMode.IncrementalBackup, Optional.empty());
    var receiver =
        new OSyncState(
            dbId, syncId, nodeFrom, nodeTo, OSyncMode.IncrementalBackup, Optional.empty());
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
    try {
      ctx1.receiveSync("test", receiver, input, OrientDBConfig.defaultConfig());
      fail("Should fail to restore");
    } catch (ODatabaseException e) {
      // TODO: handle exception
    }
    try (var session = context1.open("test", "admin", "adminpwd")) {
      // if it can open is good, it restored the right password
      fail("Should not open not synched");
    } catch (OOfflineNodeException | ODatabaseException e) {
      // TODO: it should be database exception, getting offline one ... fix
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
