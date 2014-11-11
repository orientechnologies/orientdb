package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;

/**
 * Created by emrul on 25/10/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public interface OToken {

  public boolean getIsVerified();

  public void setIsVerified(boolean verified);

  public boolean getIsValid();

  public void setIsValid(boolean valid);

  public String getSubject();

  public OUser getUser(ODatabaseDocumentInternal db);

}
