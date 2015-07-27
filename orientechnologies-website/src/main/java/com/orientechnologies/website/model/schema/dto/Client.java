package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public class Client {

  protected String  name;
  protected String  id;
  protected Integer clientId;

  protected boolean support;
  protected boolean supported;

  protected String  supportEmail;

  protected String  supportSubject;

  protected String  supportSubjectUpdate;

  protected String  supportTemplate;

  public Integer getClientId() {
    return clientId;
  }

  public void setClientId(Integer clientId) {
    this.clientId = clientId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public boolean isSupport() {
    return support;
  }

  public void setSupport(boolean support) {
    this.support = support;
  }

  public String getSupportEmail() {
    return supportEmail;
  }

  public void setSupportEmail(String supportEmail) {
    this.supportEmail = supportEmail;
  }

  public String getSupportSubject() {
    return supportSubject;
  }

  public void setSupportSubject(String supportSubject) {
    this.supportSubject = supportSubject;
  }

  public String getSupportTemplate() {
    return supportTemplate;
  }

  public void setSupported(boolean supported) {
    this.supported = supported;
  }

  public boolean isSupported() {
    return supported;
  }

  public void setSupportTemplate(String supportTemplate) {
    this.supportTemplate = supportTemplate;
  }

  public String getSupportSubjectUpdate() {
    return supportSubjectUpdate;
  }

  public void setSupportSubjectUpdate(String supportSubjectUpdate) {
    this.supportSubjectUpdate = supportSubjectUpdate;
  }
}
