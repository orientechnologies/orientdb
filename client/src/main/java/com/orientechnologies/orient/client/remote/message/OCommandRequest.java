package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OCommandRequest implements OBinaryRequest {
  private final boolean             asynch;
  private final OCommandRequestText iCommand;
  private final boolean             live;

  public OCommandRequest(boolean asynch, OCommandRequestText iCommand, boolean live) {
    this.asynch = asynch;
    this.iCommand = iCommand;
    this.live = live;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    if (live) {
      network.writeByte((byte) 'l');
    } else {
      network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
    }
    network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(iCommand));

  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_COMMAND;
  }
}