package com.orientechnologies.website.model.schema.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 12/05/15.
 */
public class Contract {

  private String                    id;
  private String                    name;
  private List<String> businessHours = new ArrayList<String>();
  private Map<Integer, Integer>     slas          = new HashMap<Integer, Integer>();

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
}
