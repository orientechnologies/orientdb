package com.orientechnologies.website.model.schema.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 17/10/14.
 */
public class MessageError {

  private String      msg;
  private List<Error> errors = new ArrayList<Error>();

  public MessageError(String msg) {

    this.msg = msg;
  }

  public void addError(String code, String desc) {
    errors.add(new Error(code, desc));
  }
}
