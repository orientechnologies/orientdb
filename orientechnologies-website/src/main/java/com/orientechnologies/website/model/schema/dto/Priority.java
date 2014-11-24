package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 24/11/14.
 */
public class Priority {

  private String  id;
  private String  name;
  private Integer number;
  private String  color;

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

  public Integer getNumber() {
    return number;
  }

  public void setNumber(Integer number) {
    this.number = number;
  }

  public String getColor() {
    return color;
  }

  public void setColor(String color) {
    this.color = color;
  }
}
