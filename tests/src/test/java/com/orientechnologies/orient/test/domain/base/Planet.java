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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.Id;
import javax.persistence.Version;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class Planet {

  @Id private String id;

  @Version private Integer version;

  private String name;

  private int distanceSun;

  private List<Satellite> satellites = new ArrayList<Satellite>();

  private Map<String, Satellite> satellitesMap = new HashMap<String, Satellite>();

  public Planet() {}

  Planet(String iName, int iDistance) {
    name = iName;
    distanceSun = iDistance;
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

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getDistanceSun() {
    return distanceSun;
  }

  public void setDistanceSun(int distanceSun) {
    this.distanceSun = distanceSun;
  }

  public List<Satellite> getSatellites() {
    return satellites;
  }

  public void setSatellites(List<Satellite> satellites) {
    this.satellites = satellites;
  }

  public Map<String, Satellite> getSatellitesMap() {
    return satellitesMap;
  }

  public void setSatellitesMap(Map<String, Satellite> satellitesMap) {
    this.satellitesMap = satellitesMap;
  }

  public void addSatelliteMap(Satellite satellite) {
    getSatellitesMap().put(satellite.getName(), satellite);
  }

  public void removeSatelliteMap(Satellite satellite) {
    getSatellitesMap().remove(satellite.getName());
  }

  public void addSatellite(Satellite satellite) {
    getSatellites().add(satellite);
  }

  public void removeSatellite(Satellite satellite) {
    getSatellites().remove(satellite);
  }
}
