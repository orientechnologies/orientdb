/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.exception.OSerializationException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Set;

public class OStorageRemoteConfiguration extends OStorageConfiguration {

  private static final long serialVersionUID = -3850696054909943272L;
  private String networkRecordSerializer;

  public OStorageRemoteConfiguration(OStorageRemote oStorageRemote, String iRecordSerializer) {
    super(oStorageRemote, Charset.forName("UTF-8"));

    networkRecordSerializer = iRecordSerializer;
  }

  @Override
  public String getRecordSerializer() {
    return networkRecordSerializer;
  }

  @Override
  public void create() throws IOException {
    throw new UnsupportedOperationException("create");
  }

//  @Override
//  public void update() throws OSerializationException {
//    throw new UnsupportedOperationException("update");
//  }

  @Override
  public void delete() throws IOException {
    throw new UnsupportedOperationException( "delete");
  }

  @Override
  public IndexEngineData getIndexEngine(String name) {
    throw new UnsupportedOperationException("get index engine");
  }

  @Override
  public void deleteIndexEngine(String name) {
    throw new UnsupportedOperationException("delete index engine");
  }

  @Override
  public Set<String> indexEngines() {
    throw new UnsupportedOperationException("index engines");
  }

  @Override
  public void addIndexEngine(String name, IndexEngineData engineData) {
    throw new UnsupportedOperationException("add index engine");
  }

  @Override
  public void dropCluster(int iClusterId) {
    throw new UnsupportedOperationException("drop cluster");
  }

  @Override
  public void setClusterStatus(int clusterId, OStorageClusterConfiguration.STATUS iStatus) {
    throw new UnsupportedOperationException("cluster status");
  }

  @Override
  public void removeProperty(String iName) {
    throw new UnsupportedOperationException("remove property");
  }


}
