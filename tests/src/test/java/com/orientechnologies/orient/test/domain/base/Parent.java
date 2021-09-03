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

import javax.persistence.Embedded;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class Parent {
  private String name;

  @Embedded private EmbeddedChild embeddedChild;

  private EmbeddedChild child;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public EmbeddedChild getChild() {
    return child;
  }

  public void setChild(EmbeddedChild child) {
    this.child = child;
  }

  public EmbeddedChild getEmbeddedChild() {
    return embeddedChild;
  }

  public void setEmbeddedChild(EmbeddedChild embeddedChild) {
    this.embeddedChild = embeddedChild;
  }

  @Override
  public String toString() {
    return "Parent [getName()=" + getName() + "]";
  }
}
