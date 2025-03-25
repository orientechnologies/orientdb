package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

  protected OClass findClass(OCommandContext ctx) {

    OSchema schema =
        ((OMetadataInternal) ctx.getDatabase().getMetadata()).getImmutableSchemaSnapshot();

    OClass clazz = schema.getClass(this.clazz);
    if (clazz == null) {
      clazz = schema.getView(this.clazz);
    }
    if (clazz == null) {
      throw new OCommandExecutionException("Cannot find class " + clazz);
    }
    return clazz;
  }

  private PrePath findPrePath(OPath path, OCommandContext ctx) {
    List<String> rawPath = path.getPath();
    String lastP = rawPath.remove(rawPath.size() - 1);
    PrePath cand =
        new PrePath() {
          {
            chain = Optional.empty();
            this.cl = OClassIndexFinder.this.findClass(ctx);
            valid = true;
            last = lastP;
          }
        };
    for (String ele : rawPath) {
      OProperty prop = cand.cl.getProperty(ele);
      if (prop != null) {
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
      } else {
        cand.valid = false;
        return cand;
      }
    }
    return cand;
  }

  @Override
  public Optional<OIndexCandidate> findAny(OPath path, OIndexKeySource value, OCommandContext ctx) {
    return findExact(path, value, ctx, true);
  }

  @Override
  public Optional<OIndexCandidate> findExact(
      OPath path, OIndexKeySource value, OCommandContext ctx) {
    return findExact(path, value, ctx, false);
  }

  public Optional<OIndexCandidate> findExact(
      OPath path, OIndexKeySource value, OCommandContext ctx, boolean requireDistinct) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    Collection<OIndex> indexes = findIndexes(cl, last);
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators()
              && index.getDefinition().getFields().size() == 1
          || !index.getDefinition().isNullValuesIgnored()) {
        OIndexCandidate candidate = newCandidate(value, last, Operation.Eq, index, requireDistinct);
        if (cand.isPresent()) {
          ((OIndexCandidateChain) cand.get()).add(index.getName());
          ((OIndexCandidateChain) cand.get()).setOperation(Operation.Eq);
          return cand;
        } else {
          return Optional.of(candidate);
        }
      }
    }
    return Optional.empty();
  }

  protected OIndexCandidate newCandidate(
      OIndexKeySource value,
      String prop,
      Operation operation,
      OIndex index,
      boolean requireDistinct) {
    OIndexCandidate candidate;
    if (index.getDefinition().getFields().size() > 1) {
      candidate =
          new OIndexCandidateComposite(index.getName(), operation, prop, value, requireDistinct);
    } else {
      candidate = new OIndexCandidateOne(index.getName(), operation, prop, value, requireDistinct);
    }
    return candidate;
  }

  @Override
  public Optional<OIndexCandidate> findNull(OPath path, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    Collection<OIndex> indexes = findIndexes(cl, last);
    for (OIndex index : indexes) {
      if (!index.getDefinition().isNullValuesIgnored()) {
        OIndexCandidate candidate =
            newCandidate(
                (c, asc) -> {
                  return Collections.singleton(null);
                },
                last,
                Operation.Eq,
                index,
                false);
        if (cand.isPresent()) {
          ((OIndexCandidateChain) cand.get()).add(index.getName());
          ((OIndexCandidateChain) cand.get()).setOperation(Operation.Eq);
          return cand;
        } else {
          return Optional.of(candidate);
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByKey(
      OPath path, OIndexKeySource value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    OProperty prop = cl.getProperty(last);
    if (prop != null) {
      if (prop.getType() == OType.EMBEDDEDMAP) {
        Collection<OIndex> indexes = cl.getClassInvolvedIndexes(last);
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
                  OIndexCandidate candidate = newCandidate(value, last, Operation.Eq, index, false);
                  return Optional.of(candidate);
                }
              }
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findAllowRange(
      OPath path, Operation op, OIndexKeySource value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    Collection<OIndex> indexes = findIndexes(cl, last);
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators() && index.supportsOrderedIterations()) {
        OIndexCandidate candidate = newCandidate(value, last, op, index, false);
        if (cand.isPresent()) {
          ((OIndexCandidateChain) cand.get()).add(index.getName());
          ((OIndexCandidateChain) cand.get()).setOperation(op);
          return cand;
        } else {
          return Optional.of(candidate);
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findRange(
      OPath path, OIndexKeySource first, OIndexKeySource second, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;
    Collection<OIndex> indexes = findIndexes(cl, last);
    for (OIndex index : indexes) {
      if (index.getInternal().canBeUsedInEqualityOperators() && index.supportsOrderedIterations()) {
        OProperty prop = cl.getProperty(last);
        if (cand.isPresent()) {
          ((OIndexCandidateChain) cand.get()).add(index.getName());
          ((OIndexCandidateChain) cand.get()).setOperation(Operation.Range);
          return cand;
        } else {
          return Optional.of(
              new OIndexCanditateRange(
                  index.getName(), prop.getName(), Operation.Le, first, Operation.Ge, second));
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findByValue(
      OPath path, OIndexKeySource value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    OProperty prop = cl.getProperty(last);
    if (prop != null) {
      if (prop.getType() == OType.EMBEDDEDMAP) {
        Collection<OIndex> indexes = cl.getClassInvolvedIndexes(last);
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
                  OIndexCandidate candidate = newCandidate(value, last, Operation.Eq, index, false);
                  return Optional.of(candidate);
                }
              }
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OIndexCandidate> findFullText(
      OPath path, OIndexKeySource value, OCommandContext ctx) {
    PrePath pre = findPrePath(path, ctx);
    if (!pre.valid) {
      return Optional.empty();
    }
    OClass cl = pre.cl;
    Optional<OIndexCandidate> cand = pre.chain;
    String last = pre.last;

    Collection<OIndex> indexes = findIndexesFullText(cl, last);
    for (OIndex index : indexes) {

      OProperty prop = cl.getProperty(last);
      if (cand.isPresent()) {
        ((OIndexCandidateChain) cand.get()).add(index.getName());
        ((OIndexCandidateChain) cand.get()).setOperation(Operation.FuzzyEq);
        return cand;
      } else {
        return Optional.of(
            new OIndexCandidateOne(
                index.getName(), Operation.FuzzyEq, prop.getName(), value, false));
      }
    }
    return Optional.empty();
  }

  protected Collection<OIndex> findIndexesFullText(OClass cl, String field) {
    // Move this logic to a custom method of the schema snapshot, computed at snapshot
    Collection<OIndex> indexes = cl.getIndexes();
    Set<OIndex> selected = new HashSet<OIndex>();
    for (OIndex index : indexes) {
      if (index.getDefinition().getFields().contains(field)) {
        if (OClass.INDEX_TYPE.FULLTEXT.name().equalsIgnoreCase(index.getType())
            && !index.getAlgorithm().equalsIgnoreCase("LUCENE")) {
          selected.add(index);
        }
      }
    }
    return selected;
  }

  protected Collection<OIndex> findIndexes(OClass cl, String field) {
    // Move this logic to a custom method of the schema snapshot, computed at snapshot
    Collection<OIndex> indexes = cl.getIndexes();
    Set<OIndex> selected = new HashSet<OIndex>();
    for (OIndex index : indexes) {
      if (index.getDefinition().getFields().contains(field)) {
        if (!OClass.INDEX_TYPE.FULLTEXT.name().equalsIgnoreCase(index.getType())
            && !index.getAlgorithm().equalsIgnoreCase("LUCENE")) {
          selected.add(index);
        }
      }
    }
    return selected;
  }
}
