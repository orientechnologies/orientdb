package com.orientechnologies.agent;

/** Created by Enrico Risa on 29/08/2018. */
public enum EnterprisePermissions {
  SERVER_PERMISSIONS("server.permissions", "Read permission on server permissions"),
  SERVER_BACKUP("server.backup", "Read permission on server backups"),
  SERVER_METRICS("server.metrics", "Read permission on server metrics"),
  SERVER_METRICS_EDIT("server.metrics.edit", "Write permission on server metrics"),
  SERVER_BACKUP_EDIT("server.backup.edit", "Write permission on server backups"),
  SERVER_CONFIGURATION("server.configuration", "Read permission on server configuration"),
  SERVER_DISTRIBUTED("server.distributed", "Read permission on distributed server"),
  SERVER_DISTRIBUTED_EDIT("server.distributed.edit", "Write permission on distributed server"),
  SERVER_LOG("server.log", "Read permission on server logs"),
  SERVER_PROFILER("server.profiler", "Read permission on server profiler"),
  SERVER_SECURITY("server.security", "Read permission on server security configuration"),
  SERVER_SECURITY_EDIT("server.security.edit", "Write permission on server security configuration"),
  SERVER_PLUGINS("server.plugins", "Read permission on server plugins"),
  STUDIO_IMPORTER_MANAGEMENT("server.importers", "Read/Write permission on server importers");

  private String permission;
  private String description;

  EnterprisePermissions(String permission, String description) {
    this.permission = permission;
    this.description = description;
  }

  @Override
  public String toString() {
    return permission;
  }

  public String getPermission() {
    return permission;
  }

  public String getDescription() {
    return description;
  }
}
