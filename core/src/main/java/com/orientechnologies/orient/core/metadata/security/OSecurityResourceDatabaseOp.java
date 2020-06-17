package com.orientechnologies.orient.core.metadata.security;

public class OSecurityResourceDatabaseOp extends OSecurityResource {

  public static OSecurityResourceDatabaseOp DB = new OSecurityResourceDatabaseOp("database");
  public static OSecurityResourceDatabaseOp CREATE =
      new OSecurityResourceDatabaseOp("database.create");
  public static OSecurityResourceDatabaseOp COPY = new OSecurityResourceDatabaseOp("database.copy");
  public static OSecurityResourceDatabaseOp DROP = new OSecurityResourceDatabaseOp("database.drop");
  public static OSecurityResourceDatabaseOp EXISTS =
      new OSecurityResourceDatabaseOp("database.exists");
  public static OSecurityResourceDatabaseOp COMMAND =
      new OSecurityResourceDatabaseOp("database.command");
  public static OSecurityResourceDatabaseOp COMMAND_GREMLIN =
      new OSecurityResourceDatabaseOp("database.command.gremlin");
  public static OSecurityResourceDatabaseOp FREEZE =
      new OSecurityResourceDatabaseOp("database.freeze");
  public static OSecurityResourceDatabaseOp RELEASE =
      new OSecurityResourceDatabaseOp("database.release");
  public static OSecurityResourceDatabaseOp PASS_THROUGH =
      new OSecurityResourceDatabaseOp("database.passthrough");
  public static OSecurityResourceDatabaseOp BYPASS_RESTRICTED =
      new OSecurityResourceDatabaseOp("database.bypassRestricted");
  public static OSecurityResourceDatabaseOp HOOK_RECORD =
      new OSecurityResourceDatabaseOp("database.hook.record");

  private OSecurityResourceDatabaseOp(String resourceString) {
    super(resourceString);
  }
}
