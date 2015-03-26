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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OUpdatePageRecord extends OAbstractPageWALRecord {
  private OWALChangesTree changesTree;

  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(final long pageIndex, final long fileId, final OOperationUnitId operationUnitId,
      final OWALChangesTree changesTree, OLogSequenceNumber startLsn) {
    super(pageIndex, fileId, operationUnitId, startLsn);
    this.changesTree = changesTree;
  }

  public OWALChangesTree getChanges() {
    return changesTree;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += changesTree.serializedSize();

    return serializedSize;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);
    offset = changesTree.toStream(offset, content);

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    changesTree = new OWALChangesTree();
    offset = changesTree.fromStream(offset, content);

    return offset;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    final OUpdatePageRecord that = (OUpdatePageRecord) o;

    if (lsn == null && that.lsn == null)
      return true;

    if (lsn == null)
      return false;

    if (that.lsn == null)
      return false;

    if (!lsn.equals(that.lsn))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + lsn.hashCode();
    return result;
  }
}
