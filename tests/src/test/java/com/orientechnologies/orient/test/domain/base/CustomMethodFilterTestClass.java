/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.domain.base;

/**
 * @author luca.molino
 * 
 */
public class CustomMethodFilterTestClass {

  protected String standardField;

  protected String UPPERCASEFIELD;

  protected String transientNotDefinedField;

  public String getStandardField() {
    return standardField;
  }

  public void setStandardField(String standardField) {
    this.standardField = standardField;
  }

  public String getUPPERCASEFIELD() {
    return UPPERCASEFIELD;
  }

  public void setUPPERCASEFIELD(String uPPERCASEFIELD) {
    UPPERCASEFIELD = uPPERCASEFIELD;
  }

  public String getTransientNotDefinedField() {
    return transientNotDefinedField;
  }

  public void setTransientNotDefinedField(String transientNotDefinedField) {
    this.transientNotDefinedField = transientNotDefinedField;
  }

}
