package com.orientechnologies.orient.distributed.network;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OSchedulerInternal;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.OUDPUnicastConfiguration;
import com.orientechnologies.orient.distributed.impl.ONodeInternalConfiguration;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class OUDPUnicastNodeManager extends ONodeManager {

  private static final int BUFFER_SIZE = 1024;

  private DatagramSocket socket;
  private OUDPUnicastConfiguration unicastConfig;

  /**
   * @param config
   * @param oDistributedNetworkManager
   * @param taskScheduler
   */
  public OUDPUnicastNodeManager(
      ONodeConfiguration config,
      ONodeInternalConfiguration internalConfiguration,
      ODiscoveryListener oDistributedNetworkManager,
      OSchedulerInternal taskScheduler,
      OOperationLog opLog) {
    super(config, internalConfiguration, 0, taskScheduler, oDistributedNetworkManager, opLog);

    this.unicastConfig = config.getUdpUnicast();
  }

  public void start() {
    super.start();
  }

  public void stop() {
    if (running) {
      socket.close();

      super.stop();
    }
  }

  protected void initNetwork() throws IOException {
    socket = new DatagramSocket(unicastConfig.getPort());
  }

  protected void sendMessageToGroup(byte[] msg) throws IOException {
    unicastConfig
        .getDiscoveryAddresses()
        .forEach(
            x -> {
              try {
                DatagramSocket sendingSocket = new DatagramSocket();
                try {
                  InetAddress group = InetAddress.getByName(x.getAddress());
                  DatagramPacket packet = new DatagramPacket(msg, msg.length, group, x.getPort());
                  sendingSocket.send(packet);
                } finally {
                  sendingSocket.close();
                }
              } catch (Exception e) {
                OLogManager.instance()
                    .info(this, "UPD packet send failed: " + e + " - " + e.getMessage());
              }
            });
  }

  protected void receiveMessages() {
    try {
      byte[] buffer = new byte[BUFFER_SIZE];
      DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      if (socket.isClosed()) {
        return;
      }
      socket.receive(packet);
      packet.getAddress();
      OBroadcastMessage message = deserializeMessage(packet.getData());
      if (!message.group.equals(this.config.getGroupName())) {
        return;
      }
      String fromAddr = packet.getAddress().getHostAddress();
      processMessage(message, fromAddr);
    } catch (SocketException ex) {
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  protected String getLocalAddress() {
    if (this.socket.getLocalAddress() != null) {
      return this.socket.getLocalAddress().getHostAddress();
    } else {
      return "127.0.0.1";
    }
  }
}
