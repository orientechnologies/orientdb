package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OMultipleIndexCanditate implements OIndexCandidate {

  public final List<OIndexCandidate> canditates = new ArrayList<OIndexCandidate>();

  public OMultipleIndexCanditate() {}

  private OMultipleIndexCanditate(Collection<OIndexCandidate> canditates) {
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
    String name = "";
    for (OIndexCandidate oIndexCandidate : canditates) {
      name = oIndexCandidate.getName() + "|";
    }
    return name;
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
    List<OProperty> propeties = properties();
    Map<String, OIndexCandidate> newCanditates = new HashMap<>();
    for (OIndexCandidate cand : this.canditates) {
      if (!newCanditates.containsKey(cand.getName())) {
        OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(cand.getName());
        List<OProperty> foundProps = new ArrayList<>();
        for (String field : index.getDefinition().getFields()) {
          boolean found = false;
          for (OProperty property : propeties) {
            if (property.getName().equals(field)) {
              found = true;
              foundProps.add(property);
              break;
            }
          }
          if (!found) {
            break;
          }
        }
        if (foundProps.size() == 1) {
          newCanditates.put(index.getName(), cand);
        } else if (!foundProps.isEmpty()) {
          newCanditates.put(
              index.getName(),
              new OIndexCandidateComposite(index.getName(), cand.getOperation(), foundProps));
        }
      }
    }
    if (newCanditates.isEmpty()) {
      return Optional.empty();
    } else if (newCanditates.size() == 1) {
      return Optional.of(newCanditates.values().iterator().next());
    } else {
      return Optional.of(new OMultipleIndexCanditate(newCanditates.values()));
    }
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
