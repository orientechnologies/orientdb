package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.orientechnologies.website.model.schema.Identity;

/**
 * Created by enricorisa on 16/10/14.
 */
public class Organization extends Identity {

  private String  name;

  @JsonIgnore
  private Integer closingDays;

  @JsonIgnore
  private String  closingMessage;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getClosingDays() {
    return closingDays;
  }

  public void setClosingDays(Integer closingDays) {
    this.closingDays = closingDays;
  }

  public String getClosingMessage() {
    return closingMessage;
  }

  public void setClosingMessage(String closingMessage) {
    this.closingMessage = closingMessage;
  }
}
