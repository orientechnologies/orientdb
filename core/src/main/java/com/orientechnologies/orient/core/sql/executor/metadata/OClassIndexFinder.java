package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class OClassIndexFinder implements OIndexFinder {

  public OClassIndexFinder(String clazz) {
    super();
    this.clazz = clazz;
  }

  private String clazz;

  @Override
  public Optional<OIndexCandidate> findExactIndex(OPath path, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    List<String> rawPath = path.getPath();
    String last = rawPath.remove(rawPath.size() - 1);
    Optional<OIndexCandidate> cand = Optional.empty();
    for (String ele : rawPath) {
      OProperty prop = cl.getProperty(ele);
      OClass linkedClass = prop.getLinkedClass();
      Collection<OIndex> indexes = prop.getAllIndexes();
      if (prop.getType().isLink() && linkedClass != null) {
        for (OIndex index : indexes) {
          if (index.getInternal().canBeUsedInEqualityOperators()) {
            if (cand.isPresent()) {
              ((OIndexCandidateChain) cand.get()).add(index.getName());
            } else {
              cand = Optional.of(new OIndexCandidateChain(index.getName()));
            }
            cl = linkedClass;
          } else {
            return Optional.empty();
          }
        }
      } else {
        return Optional.empty();
      }
    }
    OProperty prop = cl.getProperty(last);
    Collection<OIndex> indexes = prop.getAllIndexes();
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators()) {
        if (cand.isPresent()) {
          ((OIndexCandidateChain) cand.get()).add(index.getName());
          return cand;
        } else {
          return Optional.of(new OIndexCandidateImpl(index.getName()));
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByKeyIndex(OPath fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    OProperty prop = cl.getProperty(fieldName.getPath().get(0));
    if (prop.getType() == OType.EMBEDDEDMAP) {
      Collection<OIndex> indexes = prop.getAllIndexes();
      for (OIndex index : indexes) {
        if (index.getInternal().canBeUsedInEqualityOperators()) {
          OIndexDefinition def = index.getDefinition();
          for (String o : def.getFieldsToIndex()) {
            if (o.equalsIgnoreCase(fieldName.getPath().get(0) + " by key")) {
              return Optional.of(new OIndexCandidateImpl(index.getName()));
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findAllowRangeIndex(OPath fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    Collection<OIndex> indexes = cl.getProperty(fieldName.getPath().get(0)).getAllIndexes();
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators() && index.supportsOrderedIterations()) {
        return Optional.of(new OIndexCandidateImpl(index.getName()));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByValueIndex(OPath fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    OProperty prop = cl.getProperty(fieldName.getPath().get(0));
    if (prop.getType() == OType.EMBEDDEDMAP) {
      Collection<OIndex> indexes = prop.getAllIndexes();
      for (OIndex index : indexes) {
        OIndexDefinition def = index.getDefinition();
        if (index.getInternal().canBeUsedInEqualityOperators()) {
          for (String o : def.getFieldsToIndex()) {
            if (o.equalsIgnoreCase(fieldName.getPath().get(0) + " by value")) {
              return Optional.of(new OIndexCandidateImpl(index.getName()));
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findFullTextIndex(OPath fieldName, OCommandContext ctx) {
    OClass cl = ctx.getDatabase().getClass(this.clazz);
    Collection<OIndex> indexes = cl.getProperty(fieldName.getPath().get(0)).getAllIndexes();
    for (OIndex index : indexes) {
      if (OClass.INDEX_TYPE.FULLTEXT.name().equalsIgnoreCase(index.getType())
          && !index.getAlgorithm().equalsIgnoreCase("LUCENE")) {
        return Optional.of(new OIndexCandidateImpl(index.getName()));
      }
    }
    return Optional.empty();
  }
}
