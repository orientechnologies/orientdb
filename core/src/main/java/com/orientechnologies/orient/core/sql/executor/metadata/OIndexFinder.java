package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Optional;

public interface OIndexFinder {

  Optional<OIndexCandidate> findExactIndex(String fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findByKeyIndex(String fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findAllowRangeIndex(String fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findByValueIndex(String fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findFullTextIndex(String fieldName, OCommandContext ctx);
}
