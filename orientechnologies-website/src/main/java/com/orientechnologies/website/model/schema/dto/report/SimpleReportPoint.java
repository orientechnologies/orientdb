package com.orientechnologies.website.model.schema.dto.report;

import java.util.Date;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public class SimpleReportPoint {

  protected String label;
  protected Date   date;
  protected Double value;

  public SimpleReportPoint(){

  }

  public SimpleReportPoint(String label, Date date, Double value){
    this.label = label;
    this.date = date;
    this.value = value;
  }
  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public Double getValue() {
    return value;
  }

  public void setValue(Double value) {
    this.value = value;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }
}
