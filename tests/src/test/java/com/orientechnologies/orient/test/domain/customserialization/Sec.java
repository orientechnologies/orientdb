package com.orientechnologies.orient.test.domain.customserialization;

import java.util.ArrayList;
import java.util.List;

public class Sec {
  protected List<SecurityRole> securityRoleList = new ArrayList<SecurityRole>();

  public List<SecurityRole> getSecurityRoleList() {
    return securityRoleList;
  }

  public void setSecurityRoleList(List<SecurityRole> securityRoleList) {
    this.securityRoleList = securityRoleList;
  }
}
