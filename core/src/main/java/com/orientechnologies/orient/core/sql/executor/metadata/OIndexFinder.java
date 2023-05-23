package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Optional;

public interface OIndexFinder {

  Optional<OIndexCandidate> findExactIndex(OPath fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findByKeyIndex(OPath fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findAllowRangeIndex(OPath fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findByValueIndex(OPath fieldName, OCommandContext ctx);

  Optional<OIndexCandidate> findFullTextIndex(OPath fieldName, OCommandContext ctx);
}
