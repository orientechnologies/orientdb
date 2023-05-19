package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Collection;
import java.util.Optional;

public class OClassIndexFinder implements OIndexFinder {

  public OClassIndexFinder(String clazz) {
    super();
    this.clazz = clazz;
  }

  private String clazz;

  @Override
  public Optional<OIndexCandidate> findExactIndex(String fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    Collection<OIndex> indexes = cl.getProperty(fieldName).getAllIndexes();
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators()) {
        return Optional.of(new OIndexCandidateImpl(index.getName()));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByKeyIndex(String fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    OProperty prop = cl.getProperty(fieldName);
    if (prop.getType() == OType.EMBEDDEDMAP) {
      Collection<OIndex> indexes = prop.getAllIndexes();
      for (OIndex index : indexes) {
        if (index.getInternal().canBeUsedInEqualityOperators()) {
          OIndexDefinition def = index.getDefinition();
          for (String o : def.getFieldsToIndex()) {
            if (o.equalsIgnoreCase(fieldName + " by key")) {
              return Optional.of(new OIndexCandidateImpl(index.getName()));
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findAllowRangeIndex(String fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    Collection<OIndex> indexes = cl.getProperty(fieldName).getAllIndexes();
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators() && index.supportsOrderedIterations()) {
        return Optional.of(new OIndexCandidateImpl(index.getName()));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByValueIndex(String fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    OProperty prop = cl.getProperty(fieldName);
    if (prop.getType() == OType.EMBEDDEDMAP) {
      Collection<OIndex> indexes = prop.getAllIndexes();
      for (OIndex index : indexes) {
        OIndexDefinition def = index.getDefinition();
        if (index.getInternal().canBeUsedInEqualityOperators()) {
          for (String o : def.getFieldsToIndex()) {
            if (o.equalsIgnoreCase(fieldName + " by value")) {
              return Optional.of(new OIndexCandidateImpl(index.getName()));
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findFullTextIndex(String fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    Collection<OIndex> indexes = cl.getProperty(fieldName).getAllIndexes();
    for (OIndex index : indexes) {
      if (OClass.INDEX_TYPE.FULLTEXT.name().equalsIgnoreCase(index.getType())
          && !index.getAlgorithm().equalsIgnoreCase("LUCENE")) {
        return Optional.of(new OIndexCandidateImpl(index.getName()));
      }
    }
    return Optional.empty();
  }
}
