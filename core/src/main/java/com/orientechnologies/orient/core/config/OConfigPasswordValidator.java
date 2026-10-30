package com.orientechnologies.orient.core.config;

public class OConfigPasswordValidator {
  private boolean enabled = true;

  private Boolean ignoreUUID;
  private Integer minimumLength;
  private String numberRegEx;
  private String specialRegEx;
  private String uppercaseRegEx;

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

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
}
