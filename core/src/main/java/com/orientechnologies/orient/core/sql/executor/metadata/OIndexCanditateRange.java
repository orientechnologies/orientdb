package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.sql.executor.OBetweenIndexStream;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class OIndexCanditateRange implements OIndexCandidate {

  private String name;
  private String property;
  private Operation startOperation;
  private OIndexKeySource startValue;
  private Operation endOperation;
  private OIndexKeySource endValue;
  private final PropertyValue start;
  private final PropertyValue end;

  public OIndexCanditateRange(
      String name,
      String property,
      Operation startOperation,
      OIndexKeySource startValue,
      Operation endOperation,
      OIndexKeySource endValue) {
    this.name = name;
    this.property = property;
    this.startOperation = startOperation;
    this.startValue = startValue;
    this.endOperation = endOperation;
    this.endValue = endValue;
    this.start = new PropertyValue(property, startValue, startOperation);
    this.end = new PropertyValue(property, endValue, endOperation);
  }

  public OIndexCanditateRange(
      String name, String property, OIndexCandidateOne one, OIndexCandidateOne two) {
    this.name = name;
    this.property = property;
    if (one.getOperation().isG()) {
      this.startOperation = one.getOperation();
      this.startValue = one.getValue();
      this.endOperation = two.getOperation();
      this.endValue = two.getValue();
      this.start = new PropertyValue(property, one.getValue(), one.getOperation());
      this.end = new PropertyValue(property, two.getValue(), two.getOperation());
    } else {
      this.startOperation = two.getOperation();
      this.startValue = two.getValue();
      this.endOperation = one.getOperation();
      this.endValue = one.getValue();
      this.start = new PropertyValue(property, two.getValue(), two.getOperation());
      this.end = new PropertyValue(property, one.getValue(), one.getOperation());
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    return Optional.of(this);
  }

  @Override
  public Operation getOperation() {
    return Operation.Range;
  }

  @Override
  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    return Optional.of(this);
  }

  @Override
  public List<OIndexStream> getStreams(OCommandContext ctx, boolean isOrderAsc) {
    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndexInternal index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, name).getInternal();

    return Collections.singletonList(
        new OBetweenIndexStream(
            index,
            startValue.key(ctx, isOrderAsc).iterator().next(),
            startOperation.isInclude(),
            endValue.key(ctx, isOrderAsc).iterator().next(),
            endOperation.isInclude(),
            isOrderAsc));
  }

  public boolean requiresDistinctStep(OCommandContext ctx) {
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(name);
    if (index instanceof OCompositeIndexDefinition
        && ((OCompositeIndexDefinition) index.getDefinition()).getMultiValueDefinition() != null) {
      return true;
    }
    return false;
  }

  public boolean fullySorted(List<String> orderItems, OCommandContext ctx) {
    if (orderItems.size() == 1 && orderItems.get(0).equals(property)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public List<String> properties() {
    return Collections.singletonList(this.property);
  }

  @Override
  public List<PropertyValue> values() {
    List<PropertyValue> values = new ArrayList<>();
    values.add(start);
    values.add(end);
    return values;
  }
}
