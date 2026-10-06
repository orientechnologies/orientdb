package com.orientechnologies.orient.core.config;

import java.util.List;

public class OConfigPlugins {
  private String directory;
  private boolean hotReload = false;
  // This means that will load the plugin if is in the directory,
  // no matter if has the configuration entry or not.
  private boolean dynamic = false;

  public boolean isDynamic() {
    return dynamic;
  }

  public void setDynamic(boolean dynamic) {
    this.dynamic = dynamic;
  }

  private List<OConfigPlugin> plugin;

  public String getDirectory() {
    return directory;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }

  public boolean isHotReload() {
    return hotReload;
  }

  public void setHotReload(boolean hotReload) {
    this.hotReload = hotReload;
  }

  public List<OConfigPlugin> getPlugin() {
    return plugin;
  }

  public void setPlugin(List<OConfigPlugin> plugin) {
    this.plugin = plugin;
  }
}
