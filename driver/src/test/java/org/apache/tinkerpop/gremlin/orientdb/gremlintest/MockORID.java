package org.apache.tinkerpop.gremlin.orientdb.gremlintest;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY;
import java.io.IOException;
import java.io.OutputStream;

public class MockORID implements ORID {

  private static final long serialVersionUID = 3757196678376237054L;

  private final String errorText;

  public MockORID(String string) {
    this.errorText = string;
  }

  @Override
  public ORID getIdentity() {
    return this;
  }

  @Override
  public <T extends ORecord> T getRecord() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public void lock(boolean iExclusive) {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public boolean isLocked() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public LOCKING_STRATEGY lockingStrategy() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public void unlock() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public int compareTo(OIdentifiable o) {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public int compare(OIdentifiable o1, OIdentifiable o2) {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public byte[] toStream() throws OSerializationException {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public int getClusterId() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public long getClusterPosition() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public void reset() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public boolean isPersistent() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public boolean isValid() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public boolean isNew() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public boolean isTemporary() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public ORID copy() {
    return this;
  }

  @Override
  public String next() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public ORID nextRid() {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public int toStream(OutputStream iStream) throws IOException {
    throw new IllegalArgumentException(errorText);
  }

  @Override
  public StringBuilder toString(StringBuilder iBuffer) {
    throw new IllegalArgumentException(errorText);
  }
}
