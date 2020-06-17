package com.orientechnologies.orient.client.remote;

public interface OBinaryAsyncRequest<T extends OBinaryResponse> extends OBinaryRequest<T> {

  void setMode(byte mode);

  byte getMode();
}
