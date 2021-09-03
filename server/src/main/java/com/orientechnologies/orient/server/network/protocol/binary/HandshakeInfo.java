package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;

/** Created by tglman on 29/12/16. */
public class HandshakeInfo {

  private short protocolVersion;
  private String driverName;
  private String driverVersion;
  private byte encoding;
  private byte errorEncoding;
  private ORecordSerializer serializer;

  public HandshakeInfo(
      short protocolVersion,
      String driverName,
      String driverVersion,
      byte encoding,
      byte errorEncoding) {
    this.protocolVersion = protocolVersion;
    this.driverName = driverName;
    this.driverVersion = driverVersion;
    this.encoding = encoding;
    this.errorEncoding = errorEncoding;
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

  public byte getEncoding() {
    return encoding;
  }

  public byte getErrorEncoding() {
    return errorEncoding;
  }
}
