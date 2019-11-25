package com.orientechnologies.agent.functions;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.server.OClientConnection;

/**
 * Created by Enrico Risa on 03/08/2018.
 */
public abstract class OSQLEnterpriseFunction extends OSQLFunctionAbstract {

  public OSQLEnterpriseFunction(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    OSecurityUser user = iContext.getDatabase().getUser();
    if (user != null && user.checkIfAllowed(genericPermission(), specificPermission(), ORole.PERMISSION_EXECUTE) != null) {
      return exec(this, iCurrentRecord, iCurrentResult, iParams, iContext);
    } else {
      String usr = user != null ? user.getName() : "null";
      throw new OSecurityAccessException(" User " + usr + " does not have permission to execute the operation '" + ORole
          .permissionToString(ORole.PERMISSION_EXECUTE) + "' against the resource: " + genericPermission() + "."
          + specificPermission());
    }

  }

  protected boolean sameDatabase(OClientConnection connection, OCommandContext iContext) {
    return connection.getDatabase() != null && connection.getDatabase().getName()
        .equalsIgnoreCase(iContext.getDatabase().getName());
  }

  public abstract Object exec(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext);

  public abstract ORule.ResourceGeneric genericPermission();

  public abstract String specificPermission();
}
