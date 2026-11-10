package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class ODefaultDatabaseConfig {
  private String dateFormat;
  private String dateTimeFormat;
  private String timeZone;
  private String locale;
  private int minumumCluster;
  private String clusterSelection;
  private String conflictStragegy;
  private boolean useLightweightEdges;
  private boolean validation;

  public String getDateFormat() {
    return dateFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public String getTimeZone() {
    return timeZone;
  }

  public String getLocale() {
    return locale;
  }

  public int getMinumumCluster() {
    return minumumCluster;
  }

  public String getClusterSelection() {
    return clusterSelection;
  }

  public String getConflictStragegy() {
    return conflictStragegy;
  }

  public boolean isUseLightweightEdges() {
    return useLightweightEdges;
  }

  public boolean isValidation() {
    return validation;
  }
}
