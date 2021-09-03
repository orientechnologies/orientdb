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
public class JavaSimpleArraysTestClass {
  @Id private Object id;
  @Version private Object version;

  private String[] text = new String[] {"initTest"};
  private EnumTest[] enumeration;
  private int[] numberSimple;
  private long[] longSimple;
  private double[] doubleSimple;
  private float[] floatSimple;
  private byte[] byteSimple;
  private boolean[] flagSimple;
  private Date[] dateField;

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

  public String[] getText() {
    return text;
  }

  public void setText(String[] text) {
    this.text = text;
  }

  public EnumTest[] getEnumeration() {
    return enumeration;
  }

  public void setEnumeration(EnumTest[] enumeration) {
    this.enumeration = enumeration;
  }

  public int[] getNumberSimple() {
    return numberSimple;
  }

  public void setNumberSimple(int[] numberSimple) {
    this.numberSimple = numberSimple;
  }

  public long[] getLongSimple() {
    return longSimple;
  }

  public void setLongSimple(long[] longSimple) {
    this.longSimple = longSimple;
  }

  public double[] getDoubleSimple() {
    return doubleSimple;
  }

  public void setDoubleSimple(double[] doubleSimple) {
    this.doubleSimple = doubleSimple;
  }

  public float[] getFloatSimple() {
    return floatSimple;
  }

  public void setFloatSimple(float[] floatSimple) {
    this.floatSimple = floatSimple;
  }

  public byte[] getByteSimple() {
    return byteSimple;
  }

  public void setByteSimple(byte[] byteSimple) {
    this.byteSimple = byteSimple;
  }

  public boolean[] getFlagSimple() {
    return flagSimple;
  }

  public void setFlagSimple(boolean[] flagSimple) {
    this.flagSimple = flagSimple;
  }

  public Date[] getDateField() {
    return dateField;
  }

  public void setDateField(Date[] dateField) {
    this.dateField = dateField;
  }
}
