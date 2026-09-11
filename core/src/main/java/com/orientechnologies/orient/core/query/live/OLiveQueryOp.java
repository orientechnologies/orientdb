package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OLiveQueryOp {
  private OResult before;
  private OResult after;
  private byte type;
  private ODocument originalDoc;

  public OLiveQueryOp(ODocument originalDoc, OResult before, OResult after, byte type) {
    this.originalDoc = originalDoc;
    this.type = type;
    this.before = before;
    this.after = after;
  }

  protected ODocument getOriginalDoc() {
    return originalDoc;
  }

  public byte getType() {
    return type;
  }

  public OResult getAfter() {
    return after;
  }

  public OResult getBefore() {
    return before;
  }

  public void setAfter(OResult after) {
    this.after = after;
  }

  public void setOriginalDoc(ODocument originalDoc) {
    this.originalDoc = originalDoc;
  }
}
