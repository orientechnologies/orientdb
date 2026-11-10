package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;

@JsonInclude(Include.NON_NULL)
public class OConfig {
  private String databases;
  private String backup;
  private ODefaultDatabaseConfig defaultDatabaseConfig;
  private OConfigSecurity security;
  private List<OConfigNetwork> listeners;
  private OConfigPlugins plugins;
  private OConfigDistributed distributed;
  private List<OHandlerConfig> handlers;
  private List<OHookConfig> hooks;
  // This is mostly for legacy support,
  // on the long term this should be replaced by something else.
  private List<OConfigProperty> properties;

  public void setDatabases(String databases) {
    this.databases = databases;
  }

  public String getDatabases() {
    return databases;
  }

  public void setBackup(String backup) {
    this.backup = backup;
  }

  public String getBackup() {
    return backup;
  }

  public void setPlugins(OConfigPlugins plugins) {
    this.plugins = plugins;
  }

  public List<OConfigNetwork> getListeners() {
    return listeners;
  }

  public void setListeners(List<OConfigNetwork> listeners) {
    this.listeners = listeners;
  }

  public void setProperties(List<OConfigProperty> properties) {
    this.properties = properties;
  }

  public void setHandlers(List<OHandlerConfig> handlers) {
    this.handlers = handlers;
  }

  public ODefaultDatabaseConfig getDefaultDatabaseConfig() {
    return defaultDatabaseConfig;
  }

  public void setDefaultDatabaseConfig(ODefaultDatabaseConfig defaultDatabaseConfig) {
    this.defaultDatabaseConfig = defaultDatabaseConfig;
  }

  public OConfigSecurity getSecurity() {
    return security;
  }

  public OConfigPlugins getPlugins() {
    return plugins;
  }

  public OConfigDistributed getDistributed() {
    return distributed;
  }

  public List<OHandlerConfig> getHandlers() {
    return handlers;
  }

  public List<OHookConfig> getHooks() {
    return hooks;
  }

  public List<OConfigProperty> getProperties() {
    return properties;
  }

  public void setSecurity(OConfigSecurity security) {
    this.security = security;
  }

  public void setDistributed(OConfigDistributed distributed) {
    this.distributed = distributed;
  }

  public void setHooks(List<OHookConfig> hooks) {
    this.hooks = hooks;
  }
}
