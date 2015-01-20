package com.orientechnologies.website.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.website.model.schema.dto.MessageError;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

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

  public String toJson() {
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayOutputStream steam = new ByteArrayOutputStream();
    try {
      mapper.writeValue(steam, new Object() {
        public Integer getCode() {
          return code;
        }

        public String getMessage() {
          return ServiceException.this.getMessage();
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new String(steam.toByteArray(), Charset.forName("UTF-8"));
  }
}
