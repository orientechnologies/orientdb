/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.fs;

import com.orientechnologies.common.factory.OConfigurableStatefulFactory;
import com.orientechnologies.orient.core.exception.OConfigurationException;

import java.io.IOException;

/**
 * OFile factory. To register 3rd party implementations use: OFileFactory.instance().register(<name>, <class>);
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OFileFactory extends OConfigurableStatefulFactory<String, OFile> {
  protected static final OFileFactory instance = new OFileFactory();

  public OFileFactory() {
    register(OFileMMap.NAME, OFileMMap.class);
    register(OFileClassic.NAME, OFileClassic.class);
  }

  public static OFileFactory instance() {
    return instance;
  }

  public OFile create(final String iType, final String iFileName, final String iOpenMode) throws IOException {
    final Class<? extends OFile> fileClass = registry.get(iType);

    try {
      final OFile f = newInstance(iType);
      f.init(iFileName, iOpenMode);
      return f;
    } catch (final Exception e) {
      throw new OConfigurationException("Cannot create file of type '" + iType + "'", e);
    }
  }
}
