/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.Orient;

import java.io.IOException;

/**
 * Service thread that catches internal messages sent by the server
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OAsynchChannelServiceThread extends OSoftThread {
  private OChannelBinaryAsynchClient network;
  private int                        sessionId;
  private boolean                    tokenBased = false;
  private ORemoteServerEventListener remoteServerEventListener;

  public OAsynchChannelServiceThread(final ORemoteServerEventListener iRemoteServerEventListener,
      final OChannelBinaryAsynchClient iChannel) {
    super(Orient.instance().getThreadGroup(), "OrientDB <- Asynch Client (" + iChannel.socket.getRemoteSocketAddress() + ")");
    sessionId = Integer.MIN_VALUE;
    remoteServerEventListener = iRemoteServerEventListener;
    network = iChannel;
    start();
  }

  @Override
  protected void execute() throws Exception {
    try {
      network.beginResponse(sessionId, 0, false);
      Object obj = null;
      final byte request = network.readByte();
      switch (request) {
      case OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG:
      case OChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY:
        obj = network.readBytes();
        break;
      }

      if (remoteServerEventListener != null)
        remoteServerEventListener.onRequest(request, obj);

    } catch (IOException ioe) {
      // EXCEPTION RECEIVED (THE SOCKET HAS BEEN CLOSED?) ASSURE TO UNLOCK THE READ AND EXIT THIS THREAD
      sendShutdown();
      if (network != null) {
        final OChannelBinaryAsynchClient n = network;
        network = null;
        n.close();
      }

    } finally {
      if (network != null)
        network.endResponse();
    }
  }

  public void setTokenBased(boolean tokenBased) {
    this.tokenBased = tokenBased;
  }
}
