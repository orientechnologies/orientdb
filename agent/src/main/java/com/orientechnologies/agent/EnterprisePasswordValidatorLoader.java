package com.orientechnologies.agent;

import com.orientechnologies.orient.core.security.OPasswordValidator;
import com.orientechnologies.orient.core.security.OPasswordValidatorLoader;

public class EnterprisePasswordValidatorLoader implements OPasswordValidatorLoader {

  @Override
  public String getName() {
    return "enterprise-password-validator";
  }

  @Override
  public String getClassName() {
    return EnterprisePasswordValidator.class.getName();
  }

  @Override
  public OPasswordValidator newInstance() {
    return new EnterprisePasswordValidator();
  }
}
