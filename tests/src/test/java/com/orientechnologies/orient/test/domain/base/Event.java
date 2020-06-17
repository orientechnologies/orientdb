package com.orientechnologies.orient.test.domain.base;

import java.io.Serializable;
import java.util.Date;

public class Event implements Serializable {

  public Event() {}

  String name;
  Date date;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }
}
