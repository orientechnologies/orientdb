package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;

/**
 * Created by tglman on 29/12/16.
 */
public class HandshakeInfo {

  private short             protocolVersion;
  private String            driverName;
  private String            driverVersion;
  private ORecordSerializer serializer;

  public HandshakeInfo(short protocolVersion, String driverName, String driverVersion) {
    this.protocolVersion = protocolVersion;
    this.driverName = driverName;
    this.driverVersion = driverVersion;
    this.serializer = ORecordSerializerNetworkFactory.INSTANCE.forProtocol(protocolVersion);
  }

  public short getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(short protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public String getDriverName() {
    return driverName;
  }

  public void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  public String getDriverVersion() {
    return driverVersion;
  }

  public void setDriverVersion(String driverVersion) {
    this.driverVersion = driverVersion;
  }

  public ORecordSerializer getSerializer() {
    return serializer;
  }
}
