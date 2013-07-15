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
package com.orientechnologies.orient.core.index.hashindex.local;

import java.util.Collections;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexInternal;

/**
 * 
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OHashIndexFactory implements OIndexFactory {
  // public static final Set<String> SUPPORTED_TYPES = Collections.singleton(OUniqueHashIndex.TYPE_ID);

  @Override
  public Set<String> getTypes() {
    return Collections.emptySet();// SUPPORTED_TYPES;
  }

  @Override
  public OIndexInternal<?> createIndex(ODatabaseRecord iDatabase, String iIndexType) throws OConfigurationException {
    // if (!(iDatabase.getStorage() instanceof OStorageLocalAbstract))
    // throw new OConfigurationException("Given configuration works only for local storage.");
    //
    // final OStorageLocalAbstract storageLocal = (OStorageLocalAbstract) iDatabase.getStorage();
    // final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
    // if (directMemory == null)
    // throw new OConfigurationException("There is no suitable direct memory implementation for this platform."
    // + " Index creation was canceled.");
    //
    // if (OUniqueHashIndex.TYPE_ID.equals(iIndexType))
    // return new OUniqueHashIndex();

    throw new OConfigurationException("Unsupported type : " + iIndexType);
  }
}
