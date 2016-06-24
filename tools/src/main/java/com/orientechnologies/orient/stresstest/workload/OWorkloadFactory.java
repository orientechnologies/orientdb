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
package com.orientechnologies.orient.stresstest.workload;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Factory of workloads.
 *
 * @author Luca Garulli
 */
public class OWorkloadFactory {
  private Map<String, OWorkload> registered = new HashMap<String, OWorkload>();

  public OWorkloadFactory() {
    register(new OCRUDWorkload());
  }

  public OWorkload get(final String name) {
    return registered.get(name.toUpperCase());
  }

  public void register(final OWorkload workload) {
    registered.put(workload.getName().toUpperCase(), workload);
  }

  public Set<String> getRegistered() {
    return registered.keySet();
  }
}
