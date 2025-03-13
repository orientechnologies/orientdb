package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.executor.OBetweenIndexStream;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class OIndexCanditateRange implements OIndexCandidate {

  private String name;
  private OProperty property;
  private Operation startOperation;
  private OIndexKeySource startValue;
  private Operation endOperation;
  private OIndexKeySource endValue;

  public OIndexCanditateRange(
      String name,
      OProperty property,
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
  }

  public OIndexCanditateRange(
      String name, OProperty property, OIndexCandidateOne one, OIndexCandidateOne two) {
    this.name = name;
    this.property = property;
    if (one.getOperation().isL()) {
      this.startOperation = one.getOperation();
      this.startValue = one.getValue();
      this.endOperation = two.getOperation();
      this.endValue = two.getValue();
    } else {
      this.startOperation = two.getOperation();
      this.startValue = two.getValue();
      this.endOperation = one.getOperation();
      this.endValue = one.getValue();
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
            startValue.key(ctx).iterator().next(),
            startOperation.isInclude(),
            endValue.key(ctx).iterator().next(),
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

  public boolean fullySorted(List<String> orderItems) {
    if (orderItems.size() == 1 && orderItems.get(0).equals(property.getName())) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public List<OProperty> properties() {
    return Collections.singletonList(this.property);
  }

  @Override
  public List<OIndexKeySource> values() {
    List<OIndexKeySource> sources = new ArrayList<>();
    sources.add(startValue);
    sources.add(endValue);
    return sources;
  }
}
