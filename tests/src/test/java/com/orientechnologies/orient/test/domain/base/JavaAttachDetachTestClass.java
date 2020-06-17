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

import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.domain.business.Child;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Id;
import javax.persistence.Transient;
import javax.persistence.Version;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class JavaAttachDetachTestClass {
  public static final String testStatic = "10";
  @Transient public String testTransient;
  @Id public Object id;
  @Version public Object version;
  public ODocument embeddedDocument;
  public ODocument document;
  public OBlob byteArray;
  public String name;
  public Child specialChild;
  public Child specialChild2;
  public Map<String, Child> children = new HashMap<String, Child>();
  public List<EnumTest> enumList = new ArrayList<EnumTest>();
  public Set<EnumTest> enumSet = new HashSet<EnumTest>();
  public Map<String, EnumTest> enumMap = new HashMap<String, EnumTest>();
  public String text = "initTest";
  public EnumTest enumeration;
  public int numberSimple = 0;
  public long longSimple = 0l;
  public double doubleSimple = 0d;
  public float floatSimple = 0f;
  public byte byteSimple = 0;
  public boolean flagSimple = false;

  public String getTestTransient() {
    return testTransient;
  }

  public void setTestTransient(String testTransient) {
    this.testTransient = testTransient;
  }

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

  public ODocument getEmbeddedDocument() {
    return embeddedDocument;
  }

  public void setEmbeddedDocument(ODocument embeddedDocument) {
    this.embeddedDocument = embeddedDocument;
  }

  public ODocument getDocument() {
    return document;
  }

  public void setDocument(ODocument document) {
    this.document = document;
  }

  public OBlob getByteArray() {
    return byteArray;
  }

  public void setByteArray(OBlob byteArray) {
    this.byteArray = byteArray;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Child getSpecialChild() {
    return specialChild;
  }

  public void setSpecialChild(Child specialChild) {
    this.specialChild = specialChild;
  }

  public Child getSpecialChild2() {
    return specialChild;
  }

  public void setSpecialChild2(Child specialChild2) {
    this.specialChild2 = specialChild2;
  }

  public Map<String, Child> getChildren() {
    return children;
  }

  public void setChildren(Map<String, Child> children) {
    this.children = children;
  }

  public List<EnumTest> getEnumList() {
    return enumList;
  }

  public void setEnumList(List<EnumTest> enumList) {
    this.enumList = enumList;
  }

  public Set<EnumTest> getEnumSet() {
    return enumSet;
  }

  public void setEnumSet(Set<EnumTest> enumSet) {
    this.enumSet = enumSet;
  }

  public Map<String, EnumTest> getEnumMap() {
    return enumMap;
  }

  public void setEnumMap(Map<String, EnumTest> enumMap) {
    this.enumMap = enumMap;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public EnumTest getEnumeration() {
    return enumeration;
  }

  public void setEnumeration(EnumTest enumeration) {
    this.enumeration = enumeration;
  }

  public int getNumberSimple() {
    return numberSimple;
  }

  public void setNumberSimple(int numberSimple) {
    this.numberSimple = numberSimple;
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

  public boolean isFlagSimple() {
    return flagSimple;
  }

  public void setFlagSimple(boolean flagSimple) {
    this.flagSimple = flagSimple;
  }
}
