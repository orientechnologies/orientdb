package com.orientechnologies.agent;

/**
 * Created by Enrico Risa on 29/08/2018.
 */
public enum EnterprisePermissions {

  SERVER_PERMISSIONS("server.permissions"), SERVER_BACKUP("server.backup"), SERVER_METRICS("server.metrics"), SERVER_METRICS_EDIT(
      "server.metrics.edit"), SERVER_BACKUP_EDIT("server.backup.edit"), SERVER_CONFIGURATION(
      "server.configuration"), SERVER_DISTRIBUTED("server.distributed"), SERVER_LOG("server.log"), SERVER_PROFILER(
      "server.profiler"), SERVER_SECURITY("server.security"), SERVER_SECURITY_EDIT("server.security.edit"), SERVER_PLUGINS(
      "server.plugins"), STUDIO_DASHBOARD("server.studio.dashboard"), STUDIO_PROFILER_MANAGEMENT(
      "server.studio.profilerManagement"), STUDIO_IMPORTER_MANAGEMENT("server.studio.importerManagement");

  private String permission;

  EnterprisePermissions(String permission) {
    this.permission = permission;
  }

  @Override
  public String toString() {
    return permission;
  }
}
