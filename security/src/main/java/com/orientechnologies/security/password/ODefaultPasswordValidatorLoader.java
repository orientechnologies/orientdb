package com.orientechnologies.security.password;

import com.orientechnologies.orient.core.security.OPasswordValidator;
import com.orientechnologies.orient.core.security.OPasswordValidatorLoader;

public class ODefaultPasswordValidatorLoader implements OPasswordValidatorLoader {

  @Override
  public String getName() {
    return "default";
  }

  @Override
  public String getClassName() {
    return ODefaultPasswordValidator.class.getName();
  }

  @Override
  public OPasswordValidator newInstance() {
    return new ODefaultPasswordValidator();
  }
}
