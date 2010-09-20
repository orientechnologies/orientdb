/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.handler.cluster;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.MulticastSocket;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

public class OClusterDiscoveryListener extends OSoftThread {
  private final byte[]           recvBuffer = new byte[50000];
  private DatagramPacket         dgram;
  private OClusterNode           clusterNode;
  private OServerNetworkListener binaryNetworkListener;

  private MulticastSocket        socket;

  public OClusterDiscoveryListener(final OClusterNode iClusterNode, final OServerNetworkListener iNetworkListener) {
    super(OServer.getThreadGroup(), "DiscoveryListener");

    clusterNode = iClusterNode;
    binaryNetworkListener = iNetworkListener;

    OLogManager.instance().info(
        this,
        "Listening for cluster nodes on IP multicast " + iClusterNode.configNetworkMulticastAddress + ":"
            + iClusterNode.configNetworkMulticastPort);

    dgram = new DatagramPacket(recvBuffer, recvBuffer.length);
    try {
      socket = new MulticastSocket(iClusterNode.configNetworkMulticastPort);
      socket.joinGroup(iClusterNode.configNetworkMulticastAddress);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Can't startup cluster discovery listener", e);
    }

    start();
  }

  @Override
  protected void execute() throws Exception {
    try {
      // RESET THE LENGTH TO RE-RECEIVE THE PACKET
      dgram.setLength(recvBuffer.length);

      // BLOCKS UNTIL SOMETHING IS RECEIVED OR SOCKET SHUTDOWN
      socket.receive(dgram);

      OLogManager.instance().debug(this, "Received multicast packet %d bytes from %s:%d", dgram.getLength(), dgram.getAddress(),
          dgram.getPort());

      byte[] buffer = new byte[dgram.getLength()];
      System.arraycopy(dgram.getData(), 0, buffer, 0, buffer.length);

      try {
        String packet = new String(OSecurityManager.instance().decrypt(clusterNode.securityAlgorithm, clusterNode.securityKey,
            buffer));

        // UNPACK DATA
        String[] parts = packet.trim().split("\\|");

        int i = 0;

        if (!parts[i].startsWith(OClusterNode.PACKET_HEADER))
          return;

        if (Integer.parseInt(parts[++i]) != OClusterNode.PROTOCOL_VERSION) {
          OLogManager.instance().debug(this, "Received bad multicast packet with version %s not equals to the current %d",
              parts[i], OClusterNode.PROTOCOL_VERSION);
          return;
        }

        if (!parts[++i].equals(clusterNode.name)) {
          OLogManager.instance().debug(this, "Received bad multicast packet with cluster name %s not equals to the current %s",
              parts[i], clusterNode.name);
          return;
        }

        final String serverAddress = parts[++i];
        final int serverPort = Integer.parseInt(parts[++i]);

        // CHECK IF THE PACKET WAS SENT BY MYSELF
        if (serverAddress.equals(binaryNetworkListener.getInboundAddr().getHostName())
            && serverPort == binaryNetworkListener.getInboundAddr().getPort())
          // IT'S ME, JUST IGNORE
          return;

        // GOOD PACKET, PASS TO THE CLUSTER NODE MANAGER THIS INFO
        clusterNode.receivedClusterPresence(serverAddress, serverPort);

      } catch (Exception e) {
        // WRONG PACKET
        OLogManager.instance().debug(this, "Received wrong packet from multicast IP", e);
      }
    } catch (Throwable t) {
      OLogManager.instance().error(this, "Error on executing request", t);
    } finally {
    }
  }
}
