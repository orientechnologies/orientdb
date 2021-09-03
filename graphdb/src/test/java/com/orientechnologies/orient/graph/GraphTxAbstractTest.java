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

package com.orientechnologies.orient.graph;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import java.util.Locale;
import org.junit.AfterClass;

/**
 * Base class for tests against transactional Graphs.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class GraphTxAbstractTest {
  protected static OrientGraph graph;

  public static enum ENV {
    DEV,
    RELEASE,
    CI
  }

  public static ENV getEnvironment() {
    String envName = System.getProperty("orientdb.test.env", "dev").toUpperCase(Locale.ENGLISH);
    ENV result = null;
    try {
      result = ENV.valueOf(envName);
    } catch (IllegalArgumentException e) {
    }

    if (result == null) result = ENV.DEV;

    return result;
  }

  public static String getStorageType() {
    if (getEnvironment().equals(ENV.DEV)) return "memory";

    return "plocal";
  }

  public static void init(final String dbName) {
    if (graph == null) {
      final String storageType = getStorageType();
      final String buildDirectory = System.getProperty("buildDirectory", ".");

      System.out.println("buildDirectory = " + buildDirectory);
      final String url = System.getProperty("url");

      if (url != null) graph = new OrientGraph(url);
      else graph = new OrientGraph(storageType + ":" + buildDirectory + "/" + dbName);

      if (!graph.getRawGraph().getURL().startsWith("remote:")) {
        graph.drop();

        if (url != null) graph = new OrientGraph(url);
        else graph = new OrientGraph(storageType + ":" + buildDirectory + "/" + dbName);
      }
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (graph != null) {
      graph.shutdown();
      graph = null;
    }
  }
}
