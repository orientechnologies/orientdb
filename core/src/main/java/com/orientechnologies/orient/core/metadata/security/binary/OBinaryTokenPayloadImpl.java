package com.orientechnologies.orient.core.metadata.security.binary;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.jwt.OBinaryTokenPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenMetaInfo;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class OBinaryTokenPayloadImpl implements OBinaryTokenPayload {
  private String userName;
  private String database;
  private long expiry;
  private ORID userRid;
  private String databaseType;
  private short protocolVersion;
  private String serializer;
  private String driverName;
  private String driverVersion;
  private boolean serverUser;

  @Override
  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  @Override
  public long getExpiry() {
    return expiry;
  }

  public void setExpiry(long expiry) {
    this.expiry = expiry;
  }

  @Override
  public ORID getUserRid() {
    return userRid;
  }

  public void setUserRid(ORID rid) {
    this.userRid = rid;
  }

  @Override
  public String getDatabaseType() {
    return databaseType;
  }

  public void setDatabaseType(String databaseType) {
    this.databaseType = databaseType;
  }

  @Override
  public short getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(short protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  @Override
  public String getSerializer() {
    return serializer;
  }

  public void setSerializer(String serializer) {
    this.serializer = serializer;
  }

  @Override
  public String getDriverName() {
    return driverName;
  }

  public void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  @Override
  public String getDriverVersion() {
    return driverVersion;
  }

  public void setDriverVersion(String driverVersion) {
    this.driverVersion = driverVersion;
  }

  @Override
  public boolean isServerUser() {
    return serverUser;
  }

  public void setServerUser(boolean serverUser) {
    this.serverUser = serverUser;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  @Override
  public void serialize(DataOutputStream output, OTokenMetaInfo serializer)
      throws UnsupportedEncodingException, IOException {
    String toWrite = this.getDatabase();
    OBinaryTokenSerializer.writeString(output, toWrite);
    if (this.getDatabaseType() == null) output.writeByte(-1);
    else output.writeByte(serializer.getDbTypeID(this.getDatabaseType()));
    ORID id = this.getUserRid();
    if (id == null) {
      output.writeShort(-1);
      output.writeLong(-1);
    } else {
      output.writeShort(id.getClusterId());
      output.writeLong(id.getClusterPosition());
    }
    output.writeLong(this.getExpiry());
    output.writeBoolean(this.isServerUser());
    if (this.isServerUser()) {
      OBinaryTokenSerializer.writeString(output, this.getUserName());
    }
    output.writeShort(this.getProtocolVersion());
    OBinaryTokenSerializer.writeString(output, this.getSerializer());
    OBinaryTokenSerializer.writeString(output, this.getDriverName());
    OBinaryTokenSerializer.writeString(output, this.getDriverVersion());
  }

  @Override
  public String getPayloadType() {
    return "OrientDB";
  }
}
