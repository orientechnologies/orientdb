package com.orientechnologies.orient.client.remote.message.live;

import com.orientechnologies.orient.core.sql.executor.OResult;

/** Created by tglman on 17/05/17. */
public class OLiveQueryResult {
  public static final byte CREATE_EVENT = 1;
  public static final byte UPDATE_EVENT = 2;
  public static final byte DELETE_EVENT = 3;

  private byte eventType;
  private OResult currentValue;
  private OResult oldValue;

  public OLiveQueryResult(byte eventType, OResult currentValue, OResult oldValue) {
    this.eventType = eventType;
    this.currentValue = currentValue;
    this.oldValue = oldValue;
  }

  public byte getEventType() {
    return eventType;
  }

  public void setOldValue(OResult oldValue) {
    this.oldValue = oldValue;
  }

  public OResult getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(OResult currentValue) {
    this.currentValue = currentValue;
  }

  public void setEventType(byte eventType) {
    this.eventType = eventType;
  }

  public OResult getOldValue() {
    return oldValue;
  }
}
