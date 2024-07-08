package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.types.OModifiableLong;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.EnumMap;
import java.util.Map;

public final class ConsumptionMapEvictionIndicator extends WeakReference<Thread> {
  private final EnumMap<MemTrace, OModifiableLong> consumptionMap;

  public ConsumptionMapEvictionIndicator(
      Thread referent,
      ReferenceQueue<? super Thread> q,
      EnumMap<MemTrace, OModifiableLong> consumptionMap) {
    super(referent, q);

    this.consumptionMap = consumptionMap;
  }

  public void accumulateConsumptionStatistics(EnumMap<MemTrace, OModifiableLong> accumulator) {
    for (final Map.Entry<MemTrace, OModifiableLong> entry : this.consumptionMap.entrySet()) {
      accumulator.compute(
          entry.getKey(),
          (k, v) -> {
            if (v == null) {
              v = new OModifiableLong();
            }

            v.value += entry.getValue().value;
            return v;
          });
    }
  }
}
