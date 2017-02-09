/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.stresstest;

import com.orientechnologies.orient.client.remote.OStorageRemote;

/**
 * StressTester settings.
 *
 * @author Luca Garulli
 */
public class OStressTesterSettings {
  public String                             dbName;
  public OStressTester.OMode                mode;
  public String                             rootPassword;
  public String                             resultOutputFile;
  public String                             plocalPath;
  public int                                operationsPerTransaction;
  public int                                delay;
  public int                                concurrencyLevel;
  public String                             remoteIp;
  public boolean                            haMetrics;
  public String                             workloadCfg;
  public boolean                            keepDatabaseAfterTest;
  public int                                remotePort    = 2424;
  public boolean                            checkDatabase = false;
  public OStorageRemote.CONNECTION_STRATEGY loadBalancing = OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST;
}
