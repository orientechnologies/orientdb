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
  private boolean isMoved;

  public OStorageOperationResult(RET result) {
    this(result, false);
  }

  public OStorageOperationResult(RET result, boolean moved) {
    this.result = result;
    isMoved = moved;
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
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    result = (RET) in.readObject();
    isMoved = in.readBoolean();
  }
}
