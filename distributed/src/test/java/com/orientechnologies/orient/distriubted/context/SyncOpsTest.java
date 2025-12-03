package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.OSyncId;
import com.orientechnologies.orient.distributed.context.OSyncState;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream.RequestNext;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.distributed.db.OutputStreamMessages;
import com.orientechnologies.orient.distributed.db.OutputStreamMessages.MessageSender;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
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
    private OSyncState receiver;

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

  private void testRawSync(OSyncMode mode) {
    var syncId = new OSyncId();
    var dbId = new ODatabaseId("test");
    var nodeFrom = new ONodeId("node1");
    var nodeTo = new ONodeId("node2");

    var sender = new OSyncState(dbId, syncId, nodeFrom, nodeTo, mode);
    var receiver = new OSyncState(dbId, syncId, nodeFrom, nodeTo, mode);
    var pass = new PassTrough(sender, receiver);

    OutputStream out = new BufferedOutputStream(new OutputStreamMessages(pass, sender), 8096);
    OReceiverInputStream input = new OReceiverInputStream(pass, receiver);
    receiver.setReceiver(input);

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

  @After
  public void after() {
    context.drop("test");
    context.close();
    context1.drop("test");
    context1.close();
  }
}
