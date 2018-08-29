package com.orientechnologies.agent;

/**
 * Created by Enrico Risa on 29/08/2018.
 */
public enum EnterprisePermissions {

  STUDIO_PERMISSIONS("server.studio.permissions"), SERVER_AUDITING("server.auditing"), SERVER_BACKUP(
      "server.backup"), SERVER_CONFIGURATION("server.configuration"), SERVER_DISTRIBUTED("server.distributed"), SERVER_LOG(
      "server.log"), SERVER_PROFILER("server.profiler"), SERVER_SECURITY("server.security"), SERVER_PLUGINS("server.plugins");

  private String permission;

  EnterprisePermissions(String permission) {
    this.permission = permission;
  }

  @Override
  public String toString() {
    return permission;
  }
}
