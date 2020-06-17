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
import com.orientechnologies.orient.test.domain.business.IdentityChild;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Embedded;
import javax.persistence.Id;
import javax.persistence.Version;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class JavaComplexTestClass {
  @Id private String id;
  @Version private Object version;

  @Embedded private ODocument embeddedDocument;
  private ODocument document;
  private OBlob byteArray;
  private String name;
  private EnumTest enumField;
  private Child child;
  private Map<String, String> stringMap = new HashMap<String, String>();
  private Map<String, List<String>> stringListMap = new HashMap<String, List<String>>();
  private List<Child> list = new ArrayList<Child>();
  private Set<Child> set = new HashSet<Child>();
  private Set<IdentityChild> duplicationTestSet = new HashSet<IdentityChild>();
  private Map<String, Child> children = new HashMap<String, Child>();
  private Map<String, Object> mapObject = new HashMap<String, Object>();
  private List<EnumTest> enumList = new ArrayList<EnumTest>();
  private Set<EnumTest> enumSet = new HashSet<EnumTest>();
  private Set<String> stringSet = new HashSet<String>();
  private Map<String, EnumTest> enumMap = new HashMap<String, EnumTest>();

  @Embedded private List<Child> embeddedList = new ArrayList<Child>();
  @Embedded private Set<Child> embeddedSet = new HashSet<Child>();
  @Embedded private Map<String, Child> embeddedChildren = new HashMap<String, Child>();

  @Embedded private Map<String, Object> embeddedObjectMap = new HashMap<String, Object>();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, Child> getChildren() {
    return children;
  }

  public void setChildren(Map<String, Child> children) {
    this.children = children;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Object getVersion() {
    return version;
  }

  public void setVersion(Object version) {
    this.version = version;
  }

  public List<EnumTest> getEnumList() {
    return enumList;
  }

  public void setEnumList(List<EnumTest> enumList) {
    this.enumList = enumList;
  }

  public Map<String, EnumTest> getEnumMap() {
    return enumMap;
  }

  public void setEnumMap(Map<String, EnumTest> enumMap) {
    this.enumMap = enumMap;
  }

  public Set<EnumTest> getEnumSet() {
    return enumSet;
  }

  public void setEnumSet(Set<EnumTest> enumSet) {
    this.enumSet = enumSet;
  }

  public ODocument getEmbeddedDocument() {
    return embeddedDocument;
  }

  public void setEmbeddedDocument(ODocument embeddedDocument) {
    this.embeddedDocument = embeddedDocument;
  }

  public OBlob getByteArray() {
    return byteArray;
  }

  public void setByteArray(OBlob byteArray) {
    this.byteArray = byteArray;
  }

  public ODocument getDocument() {
    return document;
  }

  public void setDocument(ODocument document) {
    this.document = document;
  }

  public Set<Child> getSet() {
    return set;
  }

  public void setSet(Set<Child> set) {
    this.set = set;
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

  public List<Child> getList() {
    return list;
  }

  public void setList(List<Child> list) {
    this.list = list;
  }

  public void setMapObject(Map<String, Object> mapObject) {
    this.mapObject = mapObject;
  }

  public Map<String, Object> getMapObject() {
    return mapObject;
  }

  public EnumTest getEnumField() {
    return enumField;
  }

  public void setEnumField(EnumTest enumField) {
    this.enumField = enumField;
  }

  public Set<String> getStringSet() {
    return stringSet;
  }

  public void setStringSet(Set<String> stringSet) {
    this.stringSet = stringSet;
  }

  public Child getChild() {
    return child;
  }

  public void setChild(Child child) {
    this.child = child;
  }

  public Map<String, Child> getEmbeddedChildren() {
    return embeddedChildren;
  }

  public void setEmbeddedChildren(Map<String, Child> embeddedChildren) {
    this.embeddedChildren = embeddedChildren;
  }

  public List<Child> getEmbeddedList() {
    return embeddedList;
  }

  public void setEmbeddedList(List<Child> embeddedList) {
    this.embeddedList = embeddedList;
  }

  public Set<Child> getEmbeddedSet() {
    return embeddedSet;
  }

  public void setEmbeddedSet(Set<Child> embeddedSet) {
    this.embeddedSet = embeddedSet;
  }

  public Set<IdentityChild> getDuplicationTestSet() {
    return duplicationTestSet;
  }

  public void setDuplicationTestSet(Set<IdentityChild> duplicationTestSet) {
    this.duplicationTestSet = duplicationTestSet;
  }

  public Map<String, Object> getEmbeddedObjectMap() {
    return embeddedObjectMap;
  }

  public void setEmbeddedObjectMap(Map<String, Object> embeddedObjectMap) {
    this.embeddedObjectMap = embeddedObjectMap;
  }
}
