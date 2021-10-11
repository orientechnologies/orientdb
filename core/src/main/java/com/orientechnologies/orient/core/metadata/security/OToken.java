package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;

/**
 * Created by emrul on 25/10/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public interface OToken {

  OTokenHeader getHeader();

  boolean getIsVerified();

  void setIsVerified(boolean verified);

  boolean getIsValid();

  void setIsValid(boolean valid);

  String getUserName();

  OUser getUser(ODatabaseDocumentInternal db);

  String getDatabase();

  String getDatabaseType();

  ORID getUserId();

  long getExpiry();

  void setExpiry(long expiry);

  boolean isNowValid();

  boolean isCloseToExpire();
}
