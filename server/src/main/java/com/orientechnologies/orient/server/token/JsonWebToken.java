package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.jwt.OJsonWebToken;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OrientJwtHeader;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class JsonWebToken implements OJsonWebToken, OToken {

  public final OTokenHeader header;
  public final OJwtPayload payload;
  private boolean isVerified;
  private boolean isValid;

  public JsonWebToken() {
    this(new OrientJwtHeader(), new OrientJwtPayload());
  }

  public JsonWebToken(OTokenHeader header, OJwtPayload payload) {
    isVerified = false;
    isValid = false;
    this.header = header;
    this.payload = payload;
  }

  @Override
  public OTokenHeader getHeader() {
    return header;
  }

  @Override
  public OJwtPayload getPayload() {
    return payload;
  }

  @Override
  public boolean getIsVerified() {
    return isVerified;
  }

  @Override
  public void setIsVerified(boolean verified) {
    this.isVerified = verified;
  }

  @Override
  public boolean getIsValid() {
    return this.isValid;
  }

  @Override
  public void setIsValid(boolean valid) {
    this.isValid = valid;
  }

  @Override
  public String getUserName() {
    return payload.getUserName();
  }

  @Override
  public String getDatabase() {
    return getPayload().getDatabase();
  }

  @Override
  public long getExpiry() {
    return getPayload().getExpiry();
  }

  @Override
  public ORID getUserId() {
    return payload.getUserRid();
  }

  @Override
  public String getDatabaseType() {
    return getPayload().getDatabaseType();
  }

  @Override
  public OUser getUser(ODatabaseDocumentInternal db) {
    ORID userRid = payload.getUserRid();
    ODocument result;
    result = db.load(userRid, "roles:1");
    if (!ODocumentInternal.getImmutableSchemaClass(result).isOuser()) {
      result = null;
    }
    return new OUser(result);
  }

  @Override
  public void setExpiry(long expiry) {
    this.payload.setExpiry(expiry);
  }

  @Override
  public boolean isNowValid() {
    long now = System.currentTimeMillis();
    return getExpiry() > now && payload.getNotBefore() < now;
  }

  @Override
  public boolean isCloseToExpire() {
    long now = System.currentTimeMillis();
    return getExpiry() - 120000 <= now;
  }
}
