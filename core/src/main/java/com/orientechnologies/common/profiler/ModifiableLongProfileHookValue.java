package com.orientechnologies.common.profiler;

import com.orientechnologies.common.types.OModifiableLong;

public class ModifiableLongProfileHookValue implements OAbstractProfiler.OProfilerHookValue {
  private final OModifiableLong value;

  public ModifiableLongProfileHookValue(OModifiableLong value) {
    this.value = value;
  }

  @Override
  public Object getValue() {
    return value.getValue();
  }
}
