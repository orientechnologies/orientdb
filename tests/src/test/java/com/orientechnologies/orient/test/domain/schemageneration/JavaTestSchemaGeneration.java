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
package com.orientechnologies.orient.test.domain.schemageneration;

import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.domain.base.EnumTest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Embedded;
import javax.persistence.Id;
import javax.persistence.Transient;
import javax.persistence.Version;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class JavaTestSchemaGeneration {

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
  @Embedded private ODocument embeddedDocument;
  private ODocument document;
  private OBlob byteArray;
  private TestSchemaGenerationChild child;
  @Embedded private TestSchemaGenerationChild embeddedChild;
  private Map<String, String> stringMap = new HashMap<String, String>();
  private Map<String, List<String>> stringListMap = new HashMap<String, List<String>>();
  private List<TestSchemaGenerationChild> list = new ArrayList<TestSchemaGenerationChild>();
  private Set<TestSchemaGenerationChild> set = new HashSet<TestSchemaGenerationChild>();
  private Map<String, TestSchemaGenerationChild> children =
      new HashMap<String, TestSchemaGenerationChild>();
  private Map<String, Object> mapObject = new HashMap<String, Object>();
  private List<EnumTest> enumList = new ArrayList<EnumTest>();
  private Set<EnumTest> enumSet = new HashSet<EnumTest>();
  private Set<String> stringSet = new HashSet<String>();
  private Map<String, EnumTest> enumMap = new HashMap<String, EnumTest>();

  @Embedded
  private List<TestSchemaGenerationChild> embeddedList = new ArrayList<TestSchemaGenerationChild>();

  @Embedded
  private Set<TestSchemaGenerationChild> embeddedSet = new HashSet<TestSchemaGenerationChild>();

  @Embedded
  private Map<String, TestSchemaGenerationChild> embeddedChildren =
      new HashMap<String, TestSchemaGenerationChild>();

  @Transient private String tranisentText = "transientTest";

  @Transient
  private List<TestSchemaGenerationChild> transientList =
      new ArrayList<TestSchemaGenerationChild>();

  @Transient
  private Set<TestSchemaGenerationChild> transientSet = new HashSet<TestSchemaGenerationChild>();

  @Transient
  private Map<String, TestSchemaGenerationChild> transientChildren =
      new HashMap<String, TestSchemaGenerationChild>();

  @Transient private ODocument transientDocument;
  @Transient private Date transientDateField;

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

  public Date getDateField() {
    return dateField;
  }

  public void setDateField(Date dateField) {
    this.dateField = dateField;
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

  public Map<String, String> getStringMap() {
    return stringMap;
  }

  public void setStringMap(Map<String, String> stringMap) {
    this.stringMap = stringMap;
  }

  public Map<String, List<String>> getStringListMap() {
    return stringListMap;
  }

  public void setStringListMap(Map<String, List<String>> stringListMap) {
    this.stringListMap = stringListMap;
  }

  public Map<String, Object> getMapObject() {
    return mapObject;
  }

  public void setMapObject(Map<String, Object> mapObject) {
    this.mapObject = mapObject;
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

  public Set<String> getStringSet() {
    return stringSet;
  }

  public void setStringSet(Set<String> stringSet) {
    this.stringSet = stringSet;
  }

  public Map<String, EnumTest> getEnumMap() {
    return enumMap;
  }

  public void setEnumMap(Map<String, EnumTest> enumMap) {
    this.enumMap = enumMap;
  }

  public TestSchemaGenerationChild getChild() {
    return child;
  }

  public void setChild(TestSchemaGenerationChild child) {
    this.child = child;
  }

  public TestSchemaGenerationChild getEmbeddedChild() {
    return embeddedChild;
  }

  public void setEmbeddedChild(TestSchemaGenerationChild embeddedChild) {
    this.embeddedChild = embeddedChild;
  }

  public List<TestSchemaGenerationChild> getList() {
    return list;
  }

  public void setList(List<TestSchemaGenerationChild> list) {
    this.list = list;
  }

  public Set<TestSchemaGenerationChild> getSet() {
    return set;
  }

  public void setSet(Set<TestSchemaGenerationChild> set) {
    this.set = set;
  }

  public Map<String, TestSchemaGenerationChild> getChildren() {
    return children;
  }

  public void setChildren(Map<String, TestSchemaGenerationChild> children) {
    this.children = children;
  }

  public List<TestSchemaGenerationChild> getEmbeddedList() {
    return embeddedList;
  }

  public void setEmbeddedList(List<TestSchemaGenerationChild> embeddedList) {
    this.embeddedList = embeddedList;
  }

  public Set<TestSchemaGenerationChild> getEmbeddedSet() {
    return embeddedSet;
  }

  public void setEmbeddedSet(Set<TestSchemaGenerationChild> embeddedSet) {
    this.embeddedSet = embeddedSet;
  }

  public Map<String, TestSchemaGenerationChild> getEmbeddedChildren() {
    return embeddedChildren;
  }

  public void setEmbeddedChildren(Map<String, TestSchemaGenerationChild> embeddedChildren) {
    this.embeddedChildren = embeddedChildren;
  }

  public String getTranisentText() {
    return tranisentText;
  }

  public void setTranisentText(String tranisentText) {
    this.tranisentText = tranisentText;
  }

  public List<TestSchemaGenerationChild> getTransientList() {
    return transientList;
  }

  public void setTransientList(List<TestSchemaGenerationChild> transientList) {
    this.transientList = transientList;
  }

  public Set<TestSchemaGenerationChild> getTransientSet() {
    return transientSet;
  }

  public void setTransientSet(Set<TestSchemaGenerationChild> transientSet) {
    this.transientSet = transientSet;
  }

  public Map<String, TestSchemaGenerationChild> getTransientChildren() {
    return transientChildren;
  }

  public void setTransientChildren(Map<String, TestSchemaGenerationChild> transientChildren) {
    this.transientChildren = transientChildren;
  }

  public ODocument getTransientDocument() {
    return transientDocument;
  }

  public void setTransientDocument(ODocument transientDocument) {
    this.transientDocument = transientDocument;
  }

  public Date getTransientDateField() {
    return transientDateField;
  }

  public void setTransientDateField(Date transientDateField) {
    this.transientDateField = transientDateField;
  }
}
