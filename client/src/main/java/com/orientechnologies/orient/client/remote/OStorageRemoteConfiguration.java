package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.config.OStorageConfiguration;

public class OStorageRemoteConfiguration extends OStorageConfiguration {

  private static final long serialVersionUID = -3850696054909943272L;
  private String            networkRecordSerializer;

  public OStorageRemoteConfiguration(OStorageRemote oStorageRemote, String iRecordSerializer) {
    super(oStorageRemote);
    networkRecordSerializer = iRecordSerializer;
  }

  @Override
  public String getRecordSerializer() {
    return networkRecordSerializer;
  }

  @Override
  public int getRecordSerializerVersion() {
    return super.getRecordSerializerVersion();
  }

}
