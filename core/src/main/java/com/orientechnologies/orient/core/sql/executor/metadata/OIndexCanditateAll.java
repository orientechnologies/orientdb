package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OIndexCanditateAll implements OIndexCandidate {

  public final List<OIndexCandidate> canditates = new ArrayList<OIndexCandidate>();

  public OIndexCanditateAll() {}

  public void addCanditate(OIndexCandidate canditate) {
    this.canditates.add(canditate);
  }

  public List<OIndexCandidate> getCanditates() {
    return canditates;
  }

  @Override
  public String getName() {
    return String.join("|", canditates.stream().map(OIndexCandidate::getName).toList());
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
    OIndexCanditateAll newCanditates = new OIndexCanditateAll();
    for (OIndexCandidate candidate : canditates) {
      Optional<OIndexCandidate> result = candidate.normalize(ctx);
      if (result.isPresent()) {
        result = result.get().finalize(ctx);
      }
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

  @Override
  public List<OIndexKeySource> values() {
    List<OIndexKeySource> vals = new ArrayList<>();
    for (OIndexCandidate cand : this.canditates) {
      vals.addAll(cand.values());
    }
    return vals;
  }

  @Override
  public List<OIndexStream> getStreams(OCommandContext ctx, boolean isOrderAsc) {
    List<OIndexStream> streams = new ArrayList<>();
    for (OIndexCandidate c : canditates) {
      streams.addAll(c.getStreams(ctx, isOrderAsc));
    }
    return streams;
  }

  @Override
  public boolean requiresDistinctStep(OCommandContext ctx) {
    return true;
  }

  @Override
  public boolean fullySorted(List<String> properties, OCommandContext ctx) {
    return false;
  }
}
