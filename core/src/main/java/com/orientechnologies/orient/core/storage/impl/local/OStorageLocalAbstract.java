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

package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public abstract class OStorageLocalAbstract extends OStorageEmbedded {
  public OStorageLocalAbstract(String name, String filePath, String mode) {
    super(name, filePath, mode);
  }

  public abstract OStorageVariableParser getVariableParser();

  public abstract String getMode();

  public abstract String getStoragePath();

  protected abstract OPhysicalPosition updateRecord(OCluster cluster, ORecordId rid, byte[] recordContent,
      ORecordVersion recordVersion, byte recordType);

  protected abstract OPhysicalPosition createRecord(ODataLocal dataSegment, OCluster cluster, byte[] recordContent,
      byte recordType, ORecordId rid, ORecordVersion recordVersion);

  public abstract void freeze(boolean b);

  public abstract void release();

  public abstract ODiskCache getDiskCache();

  public abstract boolean wasClusterSoftlyClosed(String clusterIndexName);

  public abstract boolean check(boolean b, OCommandOutputListener dbCheckTest);
}
