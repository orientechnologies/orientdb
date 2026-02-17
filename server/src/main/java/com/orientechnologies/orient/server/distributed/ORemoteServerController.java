/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ONetworkMessage;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.concurrent.ExecutorService;

/**
 * Remote server controller. It handles the communication with remote servers in HA configuration.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORemoteServerController {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ORemoteServerController.class);
  public static final int CURRENT_PROTOCOL_VERSION = 2;
  public static final int MIN_SUPPORTED_PROTOCOL_VERSION = 2;

  private final ORemoteServerChannel[] requestChannels;
  private int requestChannelIndex = 0;

  private final ORemoteServerChannel[] responseChannels;
  private int responseChannelIndex = 0;

  public ORemoteServerController(
      final ORemoteServerAvailabilityCheck check,
      final ONodeId local,
      final ONodeId remote,
      final String url,
      final String user,
      final String passwd,
      final ExecutorService executor) {
    if (user == null) throw new IllegalArgumentException("User is null");
    if (passwd == null) throw new IllegalArgumentException("Password is null");

    logger.debugOut(
        local.getNode(), remote.getNode(), "Creating remote channel(s) to distributed server...");

    int requestCannelCount = OGlobalConfiguration.DISTRIBUTED_REQUEST_CHANNELS.getValueAsInteger();
    int responseCannelCount =
        OGlobalConfiguration.DISTRIBUTED_RESPONSE_CHANNELS.getValueAsInteger();
    requestChannels = new ORemoteServerChannel[requestCannelCount];
    for (int i = 0; i < requestChannels.length; ++i) {
      var channel =
          new ORemoteServerChannel(
              check, local, remote, url, user, passwd, CURRENT_PROTOCOL_VERSION, executor);
      requestChannels[i] = channel;
    }

    responseChannels = new ORemoteServerChannel[responseCannelCount];
    for (int i = 0; i < responseChannels.length; ++i) {
      var channel =
          new ORemoteServerChannel(
              check, local, remote, url, user, passwd, CURRENT_PROTOCOL_VERSION, executor);
      responseChannels[i] = channel;
    }
  }

  public void sendMessage(final ONetworkMessage req) {
    int idx;
    synchronized (requestChannels) {
      requestChannelIndex++;
      if (requestChannelIndex < 0) requestChannelIndex = 0;
      idx = requestChannelIndex % requestChannels.length;
    }
    requestChannels[idx].sendMessage(req);
  }

  public void sendRequest(final ODistributedRequest req) {
    int idx;
    synchronized (requestChannels) {
      requestChannelIndex++;
      if (requestChannelIndex < 0) requestChannelIndex = 0;
      idx = requestChannelIndex % requestChannels.length;
    }
    requestChannels[idx].sendRequest(req);
  }

  public void sendResponse(final ODistributedResponse response) {
    int idx;
    synchronized (responseChannels) {
      responseChannelIndex++;
      if (responseChannelIndex < 0) responseChannelIndex = 0;
      idx = responseChannelIndex % responseChannels.length;
    }
    responseChannels[idx].sendResponse(response);
  }

  public void close() {
    for (int i = 0; i < requestChannels.length; ++i) requestChannels[i].close();

    for (int i = 0; i < responseChannels.length; ++i) responseChannels[i].close();
  }

  public int getProtocolVersion() {
    return requestChannels[0].getDistributedProtocolVersion();
  }

  public void sendBinaryRequest(OBinaryRequest<?> request) {
    int idx;
    synchronized (requestChannels) {
      requestChannelIndex++;
      if (requestChannelIndex < 0) requestChannelIndex = 0;
      idx = requestChannelIndex % requestChannels.length;
    }
    requestChannels[idx].sendBinaryRequest(request);
  }
}
