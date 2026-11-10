package com.orientechnologies.orient.core.config;

public class OConfigPasswordValidator {
  private Boolean enabled = true;

  private Boolean ignoreUUID;
  private Integer minimumLength;
  private String numberRegEx;
  private String specialRegEx;
  private String uppercaseRegEx;

  public OConfigPasswordValidator() {}

  public void setIgnoreUUID(Boolean ignoreUUID) {
    this.ignoreUUID = ignoreUUID;
  }

  public void setMinimumLength(int minimumLength) {
    this.minimumLength = minimumLength;
  }

  public void setNumberRegEx(String numberRegEx) {
    this.numberRegEx = numberRegEx;
  }

  public void setSpecialRegEx(String specialRegEx) {
    this.specialRegEx = specialRegEx;
  }

  public void setUppercaseRegEx(String uppercaseRegEx) {
    this.uppercaseRegEx = uppercaseRegEx;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public Integer getMinimumLength() {
    return minimumLength;
  }

  public void setMinimumLength(Integer minimumLength) {
    this.minimumLength = minimumLength;
  }

  public Boolean getIgnoreUUID() {
    return ignoreUUID;
  }

  public String getNumberRegEx() {
    return numberRegEx;
  }

  public String getSpecialRegEx() {
    return specialRegEx;
  }

  public String getUppercaseRegEx() {
    return uppercaseRegEx;
  }
}
