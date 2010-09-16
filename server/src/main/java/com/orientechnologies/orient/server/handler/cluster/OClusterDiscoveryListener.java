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
import com.orientechnologies.orient.server.OServer;

public class OClusterDiscoveryListener extends OSoftThread {
  private final byte[]    recvBuffer = new byte[8192];
  private DatagramPacket  dgram;
  private OClusterNode    clusterNode;

  private MulticastSocket socket;

  public OClusterDiscoveryListener(final OClusterNode iClusterNode) {
    super(OServer.getThreadGroup(), "DiscoveryListener");

    clusterNode = iClusterNode;

    dgram = new DatagramPacket(recvBuffer, recvBuffer.length);
    try {
      socket = new MulticastSocket(iClusterNode.networkPort);
      socket.joinGroup(iClusterNode.networkAddress);
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

      try {
        String packet = new String(dgram.getData());
        String[] parts = packet.trim().split("\\|");

        if (!parts[0].startsWith(OClusterNode.PACKET_HEADER))
          return;

        if (Integer.parseInt(parts[1]) != OClusterNode.PROTOCOL_VERSION) {
          OLogManager.instance().debug(this, "Received bad multicast packet with version %s not equals to the current %d",
              parts[1], OClusterNode.PROTOCOL_VERSION);
          return;
        }

        if (!parts[2].equals(clusterNode.name)) {
          OLogManager.instance().debug(this, "Received bad multicast packet with cluster name %s not equals to the current %s",
              parts[2], clusterNode.name);
          return;
        }

        if (!parts[3].equals(clusterNode.password)) {
          OLogManager.instance().debug(this, "Received bad multicast packet with cluster password not equals to the current one",
              parts[3], clusterNode.password);
          return;
        }

        final String serverAddress = parts[4];
        final int serverPort = Integer.parseInt(parts[5]);

        // GOOD PACKET!
        OLogManager.instance().warn(this, "Discovered cluster node %s:%d", serverAddress, serverPort);

      } catch (Exception e) {
        // WRONG PACKET
      }
    } catch (Throwable t) {
      OLogManager.instance().error(this, "Error on executing request", t);
    } finally {
    }
  }
}
