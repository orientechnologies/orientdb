package com.orientechnologies.orient.core.metadata.security.binary;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.jwt.OBinaryTokenPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OBinaryToken implements OToken {

  private boolean valid;
  private boolean verified;
  private OTokenHeader header;
  private OBinaryTokenPayload payload;

  @Override
  public boolean getIsVerified() {
    return verified;
  }

  @Override
  public void setIsVerified(boolean verified) {
    this.verified = verified;
  }

  @Override
  public boolean getIsValid() {
    return valid;
  }

  @Override
  public void setIsValid(boolean valid) {
    this.valid = valid;
  }

  @Override
  public String getUserName() {
    return payload.getUserName();
  }

  @Override
  public OUser getUser(ODatabaseDocumentInternal db) {
    if (this.payload.getUserRid() != null) {
      ODocument result = db.load(new ORecordId(this.payload.getUserRid()), "roles:1");
      if (result != null && result.getClassName().equals(OUser.CLASS_NAME)) {
        return new OUser(result);
      }
    }
    return null;
  }

  @Override
  public String getDatabase() {
    return this.payload.getDatabase();
  }

  @Override
  public String getDatabaseType() {
    return this.getPayload().getDatabaseType();
  }

  @Override
  public ORID getUserId() {
    return this.getPayload().getUserRid();
  }

  public OTokenHeader getHeader() {
    return header;
  }

  public void setHeader(OTokenHeader header) {
    this.header = header;
  }

  @Override
  public void setExpiry(long expiry) {
    getPayload().setExpiry(expiry);
  }

  @Override
  public long getExpiry() {
    return payload.getExpiry();
  }

  public short getProtocolVersion() {
    return payload.getProtocolVersion();
  }

  public String getSerializer() {
    return payload.getSerializer();
  }

  public String getDriverName() {
    return payload.getDriverName();
  }

  public String getDriverVersion() {
    return payload.getDriverVersion();
  }

  public boolean isServerUser() {
    return getPayload().isServerUser();
  }

  @Override
  public boolean isNowValid() {
    long now = System.currentTimeMillis();
    return getExpiry() > now;
  }

  @Override
  public boolean isCloseToExpire() {
    long now = System.currentTimeMillis();
    return getExpiry() - 120000 <= now;
  }

  public OBinaryTokenPayload getPayload() {
    return payload;
  }

  public void setPayload(OBinaryTokenPayload payload) {
    this.payload = payload;
  }
}
