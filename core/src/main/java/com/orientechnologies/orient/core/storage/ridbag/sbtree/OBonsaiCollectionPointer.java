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

package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

/**
 * The pointer to a bonsai collection.
 *
 * <p>Determines where the collection is stored. Contains file id and pointer to the root bucket. Is
 * immutable.
 *
 * @see ORidBag
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 1.7rc1
 */
public class OBonsaiCollectionPointer {
  public static final OBonsaiCollectionPointer INVALID =
      new OBonsaiCollectionPointer(-1, new OBonsaiBucketPointer(-1, -1));

  private final long fileId;
  private final OBonsaiBucketPointer rootPointer;

  public OBonsaiCollectionPointer(long fileId, OBonsaiBucketPointer rootPointer) {
    this.fileId = fileId;
    this.rootPointer = rootPointer;
  }

  public long getFileId() {
    return fileId;
  }

  public OBonsaiBucketPointer getRootPointer() {
    return rootPointer;
  }

  public boolean isValid() {
    return fileId >= 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OBonsaiCollectionPointer that = (OBonsaiCollectionPointer) o;

    if (fileId != that.fileId) return false;
    if (!rootPointer.equals(that.rootPointer)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + rootPointer.hashCode();
    return result;
  }
}
