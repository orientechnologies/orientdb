package com.orientechnologies.orient.core.storage;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This class represent CRUD operation result RET is the actual result Stores addition information about command execution process
 * Flag {@code isMoved == true} indicates that operation has been executed on local OrientDB server node, {@code isMoved == false}
 * indicates that operation has been executed on remote OrientDB node. This information will help to maintain local indexes and
 * caches in consistent state
 * 
 * @author edegtyarenko
 * @since 28.09.12 13:47
 */
public class OStorageOperationResult<RET> implements Externalizable {

  private RET     result;
  private byte[]  modifiedRecordContent;
  private boolean isMoved;

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
    } else
      out.writeInt(-1);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    result = (RET) in.readObject();
    isMoved = in.readBoolean();
    final int modifiedRecordContentLength = in.readInt();
    if (modifiedRecordContentLength > -1) {
      modifiedRecordContent = new byte[modifiedRecordContentLength];
      in.read(modifiedRecordContent);
    }
  }
}
