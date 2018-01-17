package com.orientechnologies.agent.cloud;

/**
 * Created by Enrico Risa on 08/01/2018.
 */

public class CloudException extends RuntimeException {

  private Integer status;
  private String  error;
  private String  path;

  public CloudException(String path, Integer status, String message, String error) {
    super(message);
    this.path = path;
    this.status = status;
    this.error = error;
  }

  public Integer getStatus() {
    return status;
  }
}
