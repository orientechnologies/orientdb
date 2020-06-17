/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import java.util.Date;
import javax.persistence.Id;
import javax.persistence.Version;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class JavaSimpleTestClass {
  @Id private Object id;
  @Version private Object version;

  private String text = "initTest";
  private EnumTest enumeration;
  private int numberSimple = 0;
  private long longSimple = 0l;
  private double doubleSimple = 0d;
  private float floatSimple = 0f;
  private byte byteSimple = 0;
  private boolean flagSimple = false;
  private Date dateField;
  private JavaTestInterface testAnonymous =
      new JavaTestInterface() {

        public int getNumber() {
          // TODO Auto-generated method stub
          return -1;
        }
      };

  public Object getId() {
    return id;
  }

  public void setId(Object id) {
    this.id = id;
  }

  public Object getVersion() {
    return version;
  }

  public void setVersion(Object version) {
    this.version = version;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public int getNumberSimple() {
    return numberSimple;
  }

  public void setNumberSimple(int numberSimple) {
    this.numberSimple = numberSimple;
  }

  public boolean getFlagSimple() {
    return this.flagSimple;
  }

  public void setFlagSimple(boolean flagSimple) {
    this.flagSimple = flagSimple;
  }

  public long getLongSimple() {
    return longSimple;
  }

  public void setLongSimple(long longSimple) {
    this.longSimple = longSimple;
  }

  public double getDoubleSimple() {
    return doubleSimple;
  }

  public void setDoubleSimple(double doubleSimple) {
    this.doubleSimple = doubleSimple;
  }

  public float getFloatSimple() {
    return floatSimple;
  }

  public void setFloatSimple(float floatSimple) {
    this.floatSimple = floatSimple;
  }

  public byte getByteSimple() {
    return byteSimple;
  }

  public void setByteSimple(byte byteSimple) {
    this.byteSimple = byteSimple;
  }

  public EnumTest getEnumeration() {
    return enumeration;
  }

  public void setEnumeration(EnumTest enumeration) {
    this.enumeration = enumeration;
  }

  public JavaTestInterface getTestAnonymous() {
    return testAnonymous;
  }

  public void setTestAnonymous(JavaTestInterface testAnonymous) {
    this.testAnonymous = testAnonymous;
  }

  public Date getDateField() {
    return dateField;
  }

  public void setDateField(Date dateField) {
    this.dateField = dateField;
  }
}
