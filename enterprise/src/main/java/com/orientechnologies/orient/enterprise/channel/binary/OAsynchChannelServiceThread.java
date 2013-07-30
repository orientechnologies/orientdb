/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.enterprise.channel.binary;

import java.io.IOException;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Service thread that catches internal messages sent by the server
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OAsynchChannelServiceThread extends OSoftThread {
  private OChannelBinaryClient       network;
  private int                        sessionId;
  private ORemoteServerEventListener remoteServerEventListener;

  public OAsynchChannelServiceThread(final ORemoteServerEventListener iRemoteServerEventListener,
      final OChannelBinaryClient iChannel) {
    super(Orient.instance().getThreadGroup(), "OrientDB <- Asynch Client (" + iChannel.socket.getRemoteSocketAddress() + ")");
    sessionId = Integer.MIN_VALUE;
    remoteServerEventListener = iRemoteServerEventListener;
    network = iChannel;
    start();
  }

  @Override
  protected void execute() throws Exception {
    try {
      network.beginResponse(sessionId, 0);
      final byte request = network.readByte();

      Object obj = null;

      switch (request) {
      case OChannelBinaryProtocol.REQUEST_PUSH_RECORD:
        obj = (ORecordInternal<?>) OChannelBinaryProtocol.readIdentifiable(network);
        break;

      case OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG:
        obj = network.readBytes();
        break;
      }

      if (remoteServerEventListener != null)
        remoteServerEventListener.onRequest(request, obj);

    } catch (IOException ioe) {
      // EXCEPTION RECEIVED (THE SOCKET HAS BEEN CLOSED?) ASSURE TO UNLOCK THE READ AND EXIT THIS THREAD
      sendShutdown();
      if (network != null) {
        final OChannelBinaryClient n = network;
        network = null;
        n.close();
      }

    } finally {
      if (network != null)
        network.endResponse();
    }
  }
}
