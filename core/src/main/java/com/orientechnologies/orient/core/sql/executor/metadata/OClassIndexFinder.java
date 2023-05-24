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

  private static class PrePath {
    OClass cl;
    Optional<OIndexCandidate> chain;
    boolean valid;
    String last;
  }

  private PrePath findPrePath(OPath path, OCommandContext ctx) {
    List<String> rawPath = path.getPath();
    String lastP = rawPath.remove(rawPath.size() - 1);
    PrePath cand =
        new PrePath() {
          {
            chain = Optional.empty();
            this.cl = ctx.getDatabase().getClass(OClassIndexFinder.this.clazz);
            valid = true;
            last = lastP;
          }
        };
    for (String ele : rawPath) {
      OProperty prop = cand.cl.getProperty(ele);
      OClass linkedClass = prop.getLinkedClass();
      Collection<OIndex> indexes = prop.getAllIndexes();
      if (prop.getType().isLink() && linkedClass != null) {
        boolean found = false;
        for (OIndex index : indexes) {
          if (index.getInternal().canBeUsedInEqualityOperators()) {
            if (cand.chain.isPresent()) {
              ((OIndexCandidateChain) cand.chain.get()).add(index.getName());
            } else {
              cand.chain = Optional.of(new OIndexCandidateChain(index.getName()));
            }
            cand.cl = linkedClass;
            found = true;
          } else {
            cand.valid = false;
            return cand;
          }
        }
        if (!found) {
          cand.valid = false;
          return cand;
        }
      } else {
        cand.valid = false;
        return cand;
      }
    }
    return cand;
  }

  @Override
  public Optional<OIndexCandidate> findExactIndex(OPath path, Object value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    OProperty prop = cl.getProperty(last);
    Collection<OIndex> indexes = prop.getAllIndexes();
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators()) {
        if (cand.isPresent()) {
          ((OIndexCandidateChain) cand.get()).add(index.getName());
          ((OIndexCandidateChain) cand.get()).setOperation(Operation.Eq);
          return cand;
        } else {
          return Optional.of(new OIndexCandidateImpl(index.getName(), Operation.Eq));
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByKeyIndex(OPath path, Object value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    OProperty prop = cl.getProperty(last);
    if (prop.getType() == OType.EMBEDDEDMAP) {
      Collection<OIndex> indexes = prop.getAllIndexes();
      for (OIndex index : indexes) {
        if (index.getInternal().canBeUsedInEqualityOperators()) {
          OIndexDefinition def = index.getDefinition();
          for (String o : def.getFieldsToIndex()) {
            if (o.equalsIgnoreCase(last + " by key")) {
              if (cand.isPresent()) {
                ((OIndexCandidateChain) cand.get()).add(index.getName());
                ((OIndexCandidateChain) cand.get()).setOperation(Operation.Eq);
                return cand;
              } else {
                return Optional.of(new OIndexCandidateImpl(index.getName(), Operation.Eq));
              }
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findAllowRangeIndex(
      OPath path, Operation op, Object value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    Collection<OIndex> indexes = cl.getProperty(last).getAllIndexes();
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators() && index.supportsOrderedIterations()) {
        if (cand.isPresent()) {
          ((OIndexCandidateChain) cand.get()).add(index.getName());
          ((OIndexCandidateChain) cand.get()).setOperation(op);
          return cand;
        } else {
          return Optional.of(new OIndexCandidateImpl(index.getName(), op));
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByValueIndex(OPath path, Object value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    OProperty prop = cl.getProperty(last);
    if (prop.getType() == OType.EMBEDDEDMAP) {
      Collection<OIndex> indexes = prop.getAllIndexes();
      for (OIndex index : indexes) {
        OIndexDefinition def = index.getDefinition();
        if (index.getInternal().canBeUsedInEqualityOperators()) {
          for (String o : def.getFieldsToIndex()) {
            if (o.equalsIgnoreCase(last + " by value")) {
              if (cand.isPresent()) {
                ((OIndexCandidateChain) cand.get()).add(index.getName());
                ((OIndexCandidateChain) cand.get()).setOperation(Operation.Eq);
                return cand;
              } else {
                return Optional.of(new OIndexCandidateImpl(index.getName(), Operation.Eq));
              }
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findFullTextIndex(
      OPath path, Object value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    Collection<OIndex> indexes = cl.getProperty(last).getAllIndexes();
    for (OIndex index : indexes) {
      if (OClass.INDEX_TYPE.FULLTEXT.name().equalsIgnoreCase(index.getType())
          && !index.getAlgorithm().equalsIgnoreCase("LUCENE")) {
        if (cand.isPresent()) {
          ((OIndexCandidateChain) cand.get()).add(index.getName());
          ((OIndexCandidateChain) cand.get()).setOperation(Operation.FuzzyEq);
          return cand;
        } else {
          return Optional.of(new OIndexCandidateImpl(index.getName(), Operation.FuzzyEq));
        }
      }
    }
    return Optional.empty();
  }
}
