package com.orientechnologies.orient.core.osystem;

import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import java.util.List;

public interface OSystemUsers {

  OSecurityUser getSystemUser(final String username, final String dbName);

  void updateSystemUserPassword(String user, String pwd);

  OUser createSystemUser(String user, String password, List<String> roles);

  boolean existsSystemUser(String user);

  boolean dropSystemUser(String user);
}
