package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OIndexCanditateAny implements OIndexCandidate {

  public final List<OIndexCandidate> canditates = new ArrayList<OIndexCandidate>();

  public OIndexCanditateAny() {}

  private OIndexCanditateAny(Collection<OIndexCandidate> canditates) {
    this.canditates.addAll(canditates);
  }

  public void addCanditate(OIndexCandidate canditate) {
    this.canditates.add(canditate);
  }

  public List<OIndexCandidate> getCanditates() {
    return canditates;
  }

  @Override
  public String getName() {
    return String.join("&", canditates.stream().map(OIndexCandidate::getName).toList());
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
    Collection<OIndexCandidate> newCanditates = normalizeBetween(this.canditates, ctx);
    newCanditates = normalizeComposite(newCanditates, ctx);
    if (newCanditates.isEmpty()) {
      return Optional.empty();
    } else if (newCanditates.size() == 1) {
      return Optional.of(newCanditates.iterator().next());
    } else {
      return Optional.of(new OIndexCanditateAny(newCanditates));
    }
  }

  private Collection<OIndexCandidate> normalizeBetween(
      List<OIndexCandidate> canditates, OCommandContext ctx) {
    List<OIndexCandidate> newCanditates = new ArrayList<>();
    for (int i = 0; i < canditates.size(); i++) {
      boolean matched = false;
      OIndexCandidate canditate = canditates.get(i);
      List<OProperty> properties = canditate.properties();
      for (int z = canditates.size() - 1; z > i; z--) {
        OIndexCandidate lastCandidate = canditates.get(z);
        List<OProperty> lastProperties = lastCandidate.properties();
        if (properties.size() == 1
            && lastProperties.size() == 1
            && properties.get(0).getName() == lastProperties.get(0).getName()) {
          if (canditate.getOperation().canRangeWith(lastCandidate.getOperation())) {
            if (canditate instanceof OIndexCandidateOne
                && lastCandidate instanceof OIndexCandidateOne) {
              newCanditates.add(
                  new OIndexCanditateRange(
                      canditate.getName(),
                      properties.get(0),
                      (OIndexCandidateOne) canditate,
                      (OIndexCandidateOne) lastCandidate));
              canditates.remove(z);
              if (z != canditates.size()) {
                z++; // Increase so it does not decrease next iteration
              }
              matched = true;
            }
          }
        }
      }
      if (!matched) {
        newCanditates.add(canditate);
      }
    }
    return newCanditates;
  }

  private Collection<OIndexCandidate> normalizeComposite(
      Collection<OIndexCandidate> canditates, OCommandContext ctx) {
    List<String> indexes = new ArrayList<>();
    for (OIndexCandidate cand : canditates) {
      if (!cand.isChain()) {
        indexes.add(cand.getName());
      }
    }
    Map<String, OIndexCandidate> propCandidate = new HashMap<>();
    for (OIndexCandidate cand : canditates) {
      if (!cand.isChain()) {
        for (OProperty prop : cand.properties()) {
          propCandidate.put(prop.getName(), cand);
        }
      }
    }
    Map<String, OIndexCandidate> newCanditates = new HashMap<>();
    for (String indexName : indexes) {
      OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(indexName);
      List<OIndexCandidate> indexCandidates = new ArrayList<>();
      List<OProperty> propeties = new ArrayList<>();
      List<String> fields = index.getDefinition().getFields();
      for (String field : fields) {
        OIndexCandidate fieldCand = propCandidate.get(field);
        if (fieldCand != null) {
          indexCandidates.add(fieldCand);
          for (OProperty prop : fieldCand.properties()) {
            if (prop.getName().equals(field)) {
              propeties.add(prop);
            }
          }
          if (fieldCand.getOperation() != Operation.Eq) {
            break;
          }
        } else {
          break;
        }
      }
      if (indexCandidates.size() == 1) {
        Optional<OIndexCandidate> finalCand = indexCandidates.get(0).finalize(ctx);
        if (finalCand.isPresent()) {
          newCanditates.put(index.getName(), finalCand.get());
        }
      } else if (!indexCandidates.isEmpty()) {
        if (index.supportsOrderedIterations() || fields.size() == indexCandidates.size()) {

          Operation operation = indexCandidates.get(indexCandidates.size() - 1).getOperation();
          OIndexCandidateComposite candidate =
              new OIndexCandidateComposite(
                  index.getName(), operation, propeties, this.computeKeys(index, indexCandidates));
          Optional<OIndexCandidate> finalCand = candidate.finalize(ctx);

          if (finalCand.isPresent()) {
            newCanditates.put(index.getName(), finalCand.get());
          }
        }
      }
    }

    return newCanditates.values();
  }

  private List<OIndexKeySource> computeKeys(OIndex index, List<OIndexCandidate> candidates) {
    Map<String, OIndexKeySource> values = new HashMap<>();
    for (OIndexCandidate candidate : candidates) {
      values.putAll(candidate.mappedValues());
    }
    List<OIndexKeySource> sources = new ArrayList<>();
    for (String field : index.getDefinition().getFields()) {
      sources.add(values.get(field));
    }
    return sources;
  }

  @Override
  public List<OIndexStream> getStreams(OCommandContext ctx, boolean isOrderAsc) {
    List<OIndexStream> streams = new ArrayList<>();
    for (OIndexCandidate c : canditates) {
      streams.addAll(c.getStreams(ctx, isOrderAsc));
    }
    return streams;
  }

  public boolean requiresDistinctStep(OCommandContext ctx) {
    return true;
  }

  public boolean fullySorted(List<String> orderItems, OCommandContext ctx) {
    // TODO: check  if properties are unique
    List<OProperty> properties = this.properties();
    if (orderItems.size() == properties.size()) {
      Set<String> set = properties.stream().map((x) -> x.getName()).collect(Collectors.toSet());
      return set.containsAll(orderItems);
    } else {
      return false;
    }
  }

  @Override
  public Map<String, OIndexKeySource> mappedValues() {
    Map<String, OIndexKeySource> vals = new HashMap<>();
    for (OIndexCandidate cand : this.canditates) {
      vals.putAll(cand.mappedValues());
    }
    return vals;
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
