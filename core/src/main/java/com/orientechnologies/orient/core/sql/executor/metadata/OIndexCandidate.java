package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface OIndexCandidate {
  String getName();

  Optional<OIndexCandidate> invert();

  Operation getOperation();

  Optional<OIndexCandidate> normalize(OCommandContext ctx);

  default Optional<OIndexCandidate> finalize(OCommandContext ctx) {
    return Optional.of(this);
  }

  List<String> properties();

  Map<String, OIndexKeySource> mappedValues();

  default List<OIndexStream> getStreams(OCommandContext ctx, boolean isOrderAsc) {
    throw new UnsupportedOperationException();
  }

  boolean requiresDistinctStep(OCommandContext ctx);

  boolean fullySorted(List<String> properties, OCommandContext ctx);

  default boolean isChain() {
    return false;
  }
}
