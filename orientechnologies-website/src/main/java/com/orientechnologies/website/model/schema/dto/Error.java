package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 17/10/14.
 */
public class Error {

  private String code;
  private String desc;

  public Error(String code, String desc) {

    this.code = code;
    this.desc = desc;
  }

  public String getCode() {
    return code;
  }

  public String getDesc() {
    return desc;
  }
}
