package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.distributed.context.coordination.message.ONodeInfoListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ORemoteAddress {

  public record OBinaryAddress(String address) {}

  private List<OBinaryAddress> addresses =
      Collections.synchronizedList(new ArrayList<OBinaryAddress>());

  public ORemoteAddress() {}

  public void addAddresses(List<ONodeInfoListener> listeners) {
    for (var listener : listeners) {
      if (listener.protocol().equalsIgnoreCase("ONetworkProtocolBinary")) {
        this.addresses.add(new OBinaryAddress(listener.address()));
      }
    }
  }

  public List<OBinaryAddress> getAddresses() {
    return addresses;
  }
}
