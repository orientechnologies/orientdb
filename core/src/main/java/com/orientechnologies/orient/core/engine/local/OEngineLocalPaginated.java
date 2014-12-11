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

package com.orientechnologies.orient.core.engine.local;

import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OEngineLocalPaginated extends OEngineAbstract {
  public static final String NAME = "plocal";

  public OStorage createStorage(final String dbName, final Map<String, String> configuration) {
    try {
      // GET THE STORAGE
      return new OLocalPaginatedStorage(dbName, dbName, getMode(configuration));

    } catch (Throwable t) {
      OLogManager.instance().error(this,
          "Error on opening database: " + dbName + ". Current location is: " + new java.io.File(".").getAbsolutePath(), t,
          ODatabaseException.class);
    }
    return null;
  }

  public String getName() {
    return NAME;
  }

  public boolean isShared() {
    return true;
  }
}
