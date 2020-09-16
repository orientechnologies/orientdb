package com.orientechnologies.agent;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.security.OPasswordValidator;
import com.orientechnologies.orient.core.security.OSecuritySystem;

public class EnterprisePasswordValidator implements OPasswordValidator {

  @Override
  public void validatePassword(final String username, final String password)
      throws OInvalidPasswordException {

    if (username.equalsIgnoreCase(password)) {
      throw new OInvalidPasswordException("Password should not be equals to username");
    }
  }

  @Override
  public void active() {}

  @Override
  public void config(ODocument jsonConfig, OSecuritySystem security) {}

  @Override
  public void dispose() {}

  @Override
  public boolean isEnabled() {
    return true;
  }
}
