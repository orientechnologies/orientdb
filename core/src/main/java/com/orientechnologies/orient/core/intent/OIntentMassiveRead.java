package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;

public class OIntentMassiveRead implements OIntent {
  public void begin(final ODatabaseRaw iDatabase) {
  }

  public void end(final ODatabaseRaw iDatabase) {
  }

  @Override
  public OIntent copy() {
    final OIntentMassiveRead copy = new OIntentMassiveRead();
    return copy;
  }
}
