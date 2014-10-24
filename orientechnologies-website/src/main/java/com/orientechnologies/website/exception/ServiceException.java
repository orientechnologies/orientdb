package com.orientechnologies.website.exception;

import com.orientechnologies.website.model.schema.dto.MessageError;

/**
 * Created by Enrico Risa on 17/10/14.
 */
public class ServiceException extends RuntimeException {

  private Integer      code;
  private MessageError msg;

  private ServiceException(Integer code, String message) {
    super(message);
    this.code = code;
  }

  private ServiceException(Integer code) {
    this(code, null);
  }

  public ServiceException withMessage(String msg, Object... params) {

    if (params != null) {
      this.msg = new MessageError(String.format(msg, params));
    } else {
      this.msg = new MessageError(msg);
    }

    return this;
  }

  public ServiceException withError(String code, String desc) {
    this.msg.addError(code, desc);
    return this;
  }

  public static ServiceException create(Integer code, String message) {
    return new ServiceException(code, message);
  }

  public static ServiceException create(Integer code) {
    return new ServiceException(code, null);
  }
}
