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
import com.orientechnologies.orient.test.domain.business.Child;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Version;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class JavaCascadeDeleteTestClass {
  @Id private String id;
  @Version private Object version;

  @OneToOne(orphanRemoval = true)
  private JavaSimpleTestClass simpleClass;

  @OneToOne(orphanRemoval = true)
  private OBlob byteArray;

  private String name;

  @ManyToMany(cascade = {CascadeType.REMOVE})
  private Map<String, Child> children = new HashMap<String, Child>();

  @OneToMany(orphanRemoval = true)
  private List<Child> list = new ArrayList<Child>();

  @OneToMany(orphanRemoval = true)
  private Set<Child> set = new HashSet<Child>();

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

  public Set<Child> getSet() {
    return set;
  }

  public void setSet(Set<Child> enumSet) {
    this.set = enumSet;
  }

  public OBlob getByteArray() {
    return byteArray;
  }

  public void setByteArray(OBlob byteArray) {
    this.byteArray = byteArray;
  }

  public JavaSimpleTestClass getSimpleClass() {
    return simpleClass;
  }

  public void setSimpleClass(JavaSimpleTestClass simpleClass) {
    this.simpleClass = simpleClass;
  }

  public List<Child> getList() {
    return list;
  }

  public void setList(List<Child> list) {
    this.list = list;
  }
}
