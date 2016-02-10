/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.engine.local;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseLocalPaginatedStorage;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Enrico Risa on 08/02/16.
 */
public class OEnterpriseEnginePaginated extends OEngineLocalPaginated {

  public OEnterpriseEnginePaginated() {
    super();
  }

  @Override
  public OStorage createStorage(final String dbName, final Map<String, String> configuration) {

    try {
      return new OEnterpriseLocalPaginatedStorage(dbName, dbName, getMode(configuration), generateStorageId(), getReadCache());
    } catch (IOException e) {
      final String message = "Error on opening database: " + dbName + ". Current location is: "
          + new java.io.File(".").getAbsolutePath();
      OLogManager.instance().error(this, message, e);
      throw OException.wrapException(new ODatabaseException(message), e);
    }
  }

}
