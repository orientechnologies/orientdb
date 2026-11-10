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
  private Boolean useLightweightEdges;
  private Boolean validation;

  public ODefaultDatabaseConfig() {}

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

  public Boolean getUseLightweightEdges() {
    return useLightweightEdges;
  }

  public void setUseLightweightEdges(Boolean useLightweightEdges) {
    this.useLightweightEdges = useLightweightEdges;
  }

  public Boolean getValidation() {
    return validation;
  }

  public void setValidation(Boolean validation) {
    this.validation = validation;
  }

  public void setDateFormat(String dateFormat) {
    this.dateFormat = dateFormat;
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public void setTimeZone(String timeZone) {
    this.timeZone = timeZone;
  }

  public void setLocale(String locale) {
    this.locale = locale;
  }

  public void setMinumumCluster(int minumumCluster) {
    this.minumumCluster = minumumCluster;
  }

  public void setClusterSelection(String clusterSelection) {
    this.clusterSelection = clusterSelection;
  }

  public void setConflictStragegy(String conflictStragegy) {
    this.conflictStragegy = conflictStragegy;
  }
}
