package com.orientechnologies.orient.client.remote;

/**
 * Created by tglman on 12/04/16.
 */
public class OStorageRemoteNodeSession {
  private final String serverURL;
  private Integer sessionId = -1;
  private byte[]  token     = null;

  public OStorageRemoteNodeSession(String serverURL, Integer uniqueClientSessionId) {
    this.serverURL = serverURL;
    this.sessionId = uniqueClientSessionId;
  }

  public String getServerURL() {
    return serverURL;
  }

  public Integer getSessionId() {
    return sessionId;
  }

  public byte[] getToken() {
    return token;
  }

  public void setSession(Integer sessionId, byte[] token) {
    this.sessionId = sessionId;
    this.token = token;
  }

  public boolean isValid() {
    return this.sessionId >= 0;
  }
}
