package com.orientechnologies.website.model.schema.dto;

import java.util.Map;

/**
 * Created by Enrico Risa on 04/12/14.
 */
public class Sla {

  protected String  id;
  protected Integer responseTime;
  protected Integer resolveTime;

  protected Map     range;

  public Integer getResponseTime() {
    return responseTime;
  }

  public void setResponseTime(Integer responseTime) {
    this.responseTime = responseTime;
  }

  public Integer getResolveTime() {
    return resolveTime;
  }

  public void setResolveTime(Integer resolveTime) {
    this.resolveTime = resolveTime;
  }

  public Map getRange() {
    return range;
  }

  public void setRange(Map range) {
    this.range = range;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
