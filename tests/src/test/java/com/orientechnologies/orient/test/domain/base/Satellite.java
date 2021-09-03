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

import javax.persistence.Id;
import javax.persistence.Version;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class Satellite {
  @Id private String id;

  @Version private Integer version;

  private long diameter;

  private String name;

  private Planet near;

  public Satellite() {}

  public Satellite(String name, long diameter) {
    this.name = name;
    this.diameter = diameter;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public long getDiameter() {
    return diameter;
  }

  public void setDiameter(long diameter) {
    this.diameter = diameter;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Planet getNear() {
    return near;
  }

  public void setNear(Planet near) {
    this.near = near;
  }
}
