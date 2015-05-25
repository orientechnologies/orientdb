package com.orientechnologies.website.model.schema.dto;

import java.util.*;

/**
 * Created by Enrico Risa on 12/05/15.
 */
public class Contract {

  private String                id;
  private String                name;
  private List<String>          businessHours = new ArrayList<String>();
  private Map<Integer, Integer> slas          = new HashMap<Integer, Integer>();
  private String                uuid;
  private Date                  from;
  private Date                  to;


  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getId() {

    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getBusinessHours() {
    return businessHours;
  }

  public void setBusinessHours(List<String> businessHours) {
    this.businessHours = businessHours;
  }

  public Map<Integer, Integer> getSlas() {
    return slas;
  }

  public void setSlas(Map<Integer, Integer> slas) {
    this.slas = slas;
  }

  public Date getFrom() {
    return from;
  }

  public void setFrom(Date from) {

    this.from = from;
  }

  public Date getTo() {
    return to;
  }

  public void setTo(Date to) {
    this.to = to;
  }
}
