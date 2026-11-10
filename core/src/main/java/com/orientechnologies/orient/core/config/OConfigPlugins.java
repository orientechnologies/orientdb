package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class OConfigPlugins {
  private String directory;
  private Boolean hotReload;
  // This means that will load the plugin if is in the directory,
  // no matter if has the configuration entry or not.
  private Boolean dynamic;

  public String getDirectory() {
    return directory;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }

  public Boolean getHotReload() {
    return hotReload;
  }

  public void setHotReload(Boolean hotReload) {
    this.hotReload = hotReload;
  }

  public Boolean getDynamic() {
    return dynamic;
  }

  public void setDynamic(Boolean dynamic) {
    this.dynamic = dynamic;
  }
}
