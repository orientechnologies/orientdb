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
package com.orientechnologies.orient.stresstest.workload;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Factory of workloads.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OWorkloadFactory {
  private Map<String, OWorkload> registered = new HashMap<String, OWorkload>();

  public OWorkloadFactory() {
    register(new OCRUDWorkload());

    final ClassLoader orientClassLoader = OWorkloadFactory.class.getClassLoader();

    final Iterator<OWorkload> ite =
        lookupProviderWithOrientClassLoader(OWorkload.class, orientClassLoader);
    while (ite.hasNext()) {
      final OWorkload strategy = ite.next();
      register(strategy);
    }
  }

  public OWorkload get(final String name) {
    return registered.get(name.toUpperCase(Locale.ENGLISH));
  }

  public void register(final OWorkload workload) {
    registered.put(workload.getName().toUpperCase(Locale.ENGLISH), workload);
  }

  public Set<String> getRegistered() {
    return registered.keySet();
  }
}
