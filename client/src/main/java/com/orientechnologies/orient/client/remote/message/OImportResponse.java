package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OImportResponse implements OBinaryResponse<Void> {
  private List<String> messages = new ArrayList<>();

  public OImportResponse(List<String> messages) {
    this.messages = messages;
  }

  public OImportResponse() {
  }

  @Override
  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    for (String string : messages) {
      channel.writeString(string);
    }
    channel.writeString(null);
  }

  @Override
  public Void read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    String message;
    while ((message = network.readString()) != null) {
      messages.add(message);
    }
    return null;
  }

  public List<String> getMessages() {
    return messages;
  }
}