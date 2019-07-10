package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.record.OElement;

public class OSecurityPolicy {
  private OElement element;

  public OSecurityPolicy(OElement element) {
    this.element = element;
  }

  public OElement getElement() {
    return element;
  }

  public void setElement(OElement element) {
    this.element = element;
  }

  public String getName(){
    return element.getProperty("name");
  }
}
