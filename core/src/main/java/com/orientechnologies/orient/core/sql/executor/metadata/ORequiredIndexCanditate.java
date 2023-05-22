package com.orientechnologies.orient.core.sql.executor.metadata;

import java.util.ArrayList;
import java.util.List;

public class ORequiredIndexCanditate implements OIndexCandidate {

  public final List<OIndexCandidate> canditates = new ArrayList<OIndexCandidate>();

  public void addCanditate(OIndexCandidate canditate) {
    this.canditates.add(canditate);
  }

  public List<OIndexCandidate> getCanditates() {
    return canditates;
  }

  @Override
  public String getName() {
    String name = "";
    for (OIndexCandidate oIndexCandidate : canditates) {
      name = oIndexCandidate.getName() + "|";
    }
    return name;
  }
}
