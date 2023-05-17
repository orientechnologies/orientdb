package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Optional;

public class OClassIndexFinder implements OIndexFinder {

  private String clazz;

  @Override
  public Optional<OIndexCandidate> findExactIndex(String fieldName, OCommandContext ctx) {
    // TODO Auto-generated method stub
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByKeyIndex(String fieldName, OCommandContext ctx) {
    // TODO Auto-generated method stub
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findAllowRangeIndex(String fieldName, OCommandContext ctx) {
    // TODO Auto-generated method stub
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByValueIndex(String fieldName, OCommandContext ctx) {
    // TODO Auto-generated method stub
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findFullTextIndex(String fieldName, OCommandContext ctx) {
    // TODO Auto-generated method stub
    return Optional.empty();
  }
}
