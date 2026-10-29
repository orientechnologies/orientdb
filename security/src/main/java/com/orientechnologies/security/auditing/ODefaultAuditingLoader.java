package com.orientechnologies.security.auditing;

import com.orientechnologies.orient.core.security.OAuditingService;
import com.orientechnologies.orient.core.security.OAuditingServiceLoader;

public class ODefaultAuditingLoader implements OAuditingServiceLoader {

  @Override
  public String getName() {
    return "default";
  }

  @Override
  public String getClassName() {
    return ODefaultAuditing.class.getName();
  }

  @Override
  public OAuditingService newInstance() {
    return new ODefaultAuditing();
  }
}
