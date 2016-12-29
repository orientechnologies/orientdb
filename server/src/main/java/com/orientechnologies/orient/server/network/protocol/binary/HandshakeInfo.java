package com.orientechnologies.orient.server.network.protocol.binary;

/**
 * Created by tglman on 29/12/16.
 */
public class HandshakeInfo {

  private short  protocolVersion;
  private String driverName;
  private String driverVersion;

  public HandshakeInfo(short protocolVersion, String driverName, String driverVersion) {
    this.protocolVersion = protocolVersion;
    this.driverName = driverName;
    this.driverVersion = driverVersion;
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
}
