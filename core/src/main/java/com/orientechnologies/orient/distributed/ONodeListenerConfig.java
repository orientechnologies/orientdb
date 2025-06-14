package com.orientechnologies.orient.distributed;

import java.util.HashMap;
import java.util.Map;

public class ONodeListenerConfig {

  private String protocol;
  private String listen;

  public ONodeListenerConfig(String protocol, String address) {
    super();
    this.protocol = protocol;
    this.listen = address;
  }

  public ONodeListenerConfig(Map<String, String> map) {
    super();
    this.protocol = map.get("protocol");
    this.listen = map.get("listen");
  }

  public String getProtocol() {
    return protocol;
  }

  public String getListen() {
    return listen;
  }

  public void setListen(String listen) {
    this.listen = listen;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public Map<String, String> toMap() {
    Map<String, String> map = new HashMap<>();
    map.put("protocol", protocol);
    map.put("listen", listen);
    return map;
  }
}
