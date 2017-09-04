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
package com.orientechnologies.orient.server.hazelcast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Network simulator helper class.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ONetworkSimulator {
  private final Map<String, Set<String>> isolated = new HashMap<String, Set<String>>();

  private static final ONetworkSimulator instance = new ONetworkSimulator();

  private ONetworkSimulator() {
  }

  public static ONetworkSimulator getInstance() {
    return instance;
  }

  public synchronized void isolate(final String server1, final String server2) {
    Set<String> server1Set = isolated.get(server1);
    if (server1Set == null) {
      server1Set = new HashSet<String>();
      isolated.put(server1, server1Set);
    }
    server1Set.add(server2);

    Set<String> server2Set = isolated.get(server2);
    if (server2Set == null) {
      server2Set = new HashSet<String>();
      isolated.put(server2, server2Set);
    }
    server2Set.add(server1);
  }

  public synchronized boolean areIsolated(final String server1, final String server2) {
    final Set<String> server1Set = isolated.get(server1);
    if (server1Set != null && server1Set.contains(server2))
      return true;

    return false;
  }

  public synchronized void removeIsolation(final String server1, final String server2) {
    Set<String> server1Set = isolated.get(server1);
    if (server1Set != null)
      server1Set.remove(server2);

    Set<String> server2Set = isolated.get(server2);
    if (server2Set != null)
      server2Set.remove(server1);
  }

  public synchronized void reset() {
    isolated.clear();
  }
}
