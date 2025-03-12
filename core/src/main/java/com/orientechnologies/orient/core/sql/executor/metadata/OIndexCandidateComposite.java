package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OIndexCandidateComposite implements OIndexCandidate {
  private String index;
  private Operation operation;
  private List<OProperty> properties;

  public OIndexCandidateComposite(String index, Operation operation, List<OProperty> properties) {
    this.index = index;
    this.operation = operation;
    this.properties = properties;
  }

  @Override
  public String getName() {
    return index;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    return Optional.empty();
  }

  @Override
  public Operation getOperation() {
    return operation;
  }

  @Override
  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    return Optional.of(this);
  }

  @Override
  public List<OProperty> properties() {
    return properties;
  }

  public boolean requiresDistinctStep(OCommandContext ctx) {
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.index);
    if (index instanceof OCompositeIndexDefinition
        && ((OCompositeIndexDefinition) index.getDefinition()).getMultiValueDefinition() != null) {
      return true;
    }
    return false;
  }

  public boolean fullySorted(List<String> orderItems) {
    // TODO: check  if properties are unique
    List<OProperty> properties = this.properties();
    if (orderItems.size() == properties.size()) {
      Set<String> set = properties.stream().map((x) -> x.getName()).collect(Collectors.toSet());
      return set.containsAll(orderItems);
    } else {
      return false;
    }
  }
}
