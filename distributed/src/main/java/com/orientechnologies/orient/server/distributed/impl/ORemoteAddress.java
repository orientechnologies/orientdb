package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.distributed.context.coordination.message.ONodeInfoListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ORemoteAddress {

  record OBinaryAddress(String address) {}
  ;

  public List<OBinaryAddress> addresses =
      Collections.synchronizedList(new ArrayList<OBinaryAddress>());

  public ORemoteAddress() {}

  public void addAddresses(List<ONodeInfoListener> listeners) {
    for (var listener : listeners) {
      if (listener.protocol().equalsIgnoreCase("binary")) {
        this.addresses.add(new OBinaryAddress(listener.address()));
      }
    }
  }
}
