package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

  @Override
  public Optional<OIndexCandidate> invert() {
    // TODO: when handling operator invert it
    return Optional.of(this);
  }

  @Override
  public Operation getOperation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    ORequiredIndexCanditate newCanditates = new ORequiredIndexCanditate();
    for (OIndexCandidate candidate : canditates) {
      Optional<OIndexCandidate> result = candidate.normalize(ctx);
      if (result.isPresent()) {
        newCanditates.addCanditate(result.get());
      } else {
        return Optional.empty();
      }
    }
    return Optional.of(newCanditates);
  }

  @Override
  public List<OProperty> properties() {
    List<OProperty> props = new ArrayList<>();
    for (OIndexCandidate cand : this.canditates) {
      props.addAll(cand.properties());
    }
    return props;
  }
}
