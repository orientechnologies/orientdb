package com.orientechnologies.orient.core.metadata.security;

public class OSecurityResourceServerOp extends OSecurityResource {

  public static OSecurityResourceServerOp SERVER = new OSecurityResourceServerOp("server");
  public static OSecurityResourceServerOp STATUS = new OSecurityResourceServerOp("server.status");
  public static OSecurityResourceServerOp REMOVE = new OSecurityResourceServerOp("server.remove");
  public static OSecurityResourceServerOp ADMIN = new OSecurityResourceServerOp("server.admin");

  private OSecurityResourceServerOp(String resourceString) {
    super(resourceString);
  }
}
