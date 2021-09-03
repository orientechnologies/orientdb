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

package com.orientechnologies.orient.core.storage;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This class represent CRUD operation result RET is the actual result Stores addition information
 * about command execution process Flag {@code isMoved == true} indicates that operation has been
 * executed on local OrientDB server node, {@code isMoved == false} indicates that operation has
 * been executed on remote OrientDB node. This information will help to maintain local indexes and
 * caches in consistent state
 *
 * @author edegtyarenko
 * @since 28.09.12 13:47
 */
public class OStorageOperationResult<RET> implements Externalizable {

  private RET result;

  private byte[] modifiedRecordContent;
  private boolean isMoved;

  /** OStorageOperationResult void constructor as required for Exernalizable */
  public OStorageOperationResult() {}

  public OStorageOperationResult(final RET result) {
    this(result, null, false);
  }

  public OStorageOperationResult(final RET result, final boolean moved) {
    this.result = result;
    this.isMoved = moved;
  }

  public OStorageOperationResult(final RET result, final byte[] content, final boolean moved) {
    this.result = result;
    this.modifiedRecordContent = content;
    this.isMoved = moved;
  }

  public byte[] getModifiedRecordContent() {
    return modifiedRecordContent;
  }

  public boolean isMoved() {
    return isMoved;
  }

  public RET getResult() {
    return result;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeObject(result);
    out.writeBoolean(isMoved);
    if (modifiedRecordContent != null) {
      out.writeInt(modifiedRecordContent.length);
      out.write(modifiedRecordContent);
    } else out.writeInt(-1);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    result = (RET) in.readObject();
    isMoved = in.readBoolean();
    final int modifiedRecordContentLength = in.readInt();
    if (modifiedRecordContentLength > -1) {
      modifiedRecordContent = new byte[modifiedRecordContentLength];
      int bytesRead = 0;

      while (bytesRead < modifiedRecordContentLength) {
        int rb = in.read(modifiedRecordContent, bytesRead, modifiedRecordContentLength - bytesRead);

        if (rb < 0) break;

        bytesRead += rb;
      }
    }
  }
}
