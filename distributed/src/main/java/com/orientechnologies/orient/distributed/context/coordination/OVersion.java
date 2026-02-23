package com.orientechnologies.orient.distributed.context.coordination;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OVersion {
  private long version;

  public OVersion(long version) {
    this.version = version;
  }

  public OVersion next() {
    return new OVersion(this.version + 1);
  }

  public boolean promise(OVersion version) {
    return this.version + 1 == version.version;
  }

  public long getValue() {
    return this.version;
  }

  public void accept(OVersion version) {
    this.version = version.version;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (version ^ (version >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    OVersion other = (OVersion) obj;
    if (version != other.version) return false;
    return true;
  }

  @Override
  public String toString() {
    return "OVersion(" + version + ")";
  }

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeLong(version);
  }

  public static OVersion readNetwork(DataInput input) throws IOException {
    long version = input.readLong();
    return new OVersion(version);
  }
}
