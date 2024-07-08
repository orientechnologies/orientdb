package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.types.OModifiableLong;
import java.util.EnumMap;
import java.util.Set;

public final class OMemoryStatPrinter implements Runnable {
  private static final OLogger logger = OLogManager.instance().logger(OMemoryStatPrinter.class);
  private final Set<ConsumptionMapEvictionIndicator> consumptionMaps;

  public OMemoryStatPrinter(Set<ConsumptionMapEvictionIndicator> consumptionMaps) {
    this.consumptionMaps = consumptionMaps;
  }

  @Override
  public void run() {
    final EnumMap<MemTrace, OModifiableLong> accumulator = new EnumMap<>(MemTrace.class);

    for (final ConsumptionMapEvictionIndicator consumptionMap : consumptionMaps) {
      consumptionMap.accumulateConsumptionStatistics(accumulator);
    }

    final String memoryStat = printMemoryStatistics(accumulator);
    logger.infoNoDb(memoryStat);
  }

  private static String printMemoryStatistics(
      final EnumMap<MemTrace, OModifiableLong> memoryConsumptionByIntention) {
    long total = 0;
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(
        "\r\n-----------------------------------------------------------------------------\r\n");
    stringBuilder.append("Memory profiling results for OrientDB direct memory allocation\r\n");
    stringBuilder.append("Amount of memory consumed by category in bytes/Kb/Mb/Gb\r\n");
    stringBuilder.append("\r\n");

    for (final MemTrace intention : MemTrace.values()) {
      final OModifiableLong consumedMemory = memoryConsumptionByIntention.get(intention);
      stringBuilder.append(intention.name()).append(" : ");
      if (consumedMemory != null) {
        total += consumedMemory.value;

        stringBuilder
            .append(consumedMemory.value)
            .append("/")
            .append(consumedMemory.value / 1024)
            .append("/")
            .append(consumedMemory.value / (1024 * 1024))
            .append("/")
            .append(consumedMemory.value / (1024 * 1024 * 1024));
      } else {
        stringBuilder.append("0/0/0/0");
      }
      stringBuilder.append("\r\n");
    }

    stringBuilder.append("\r\n");

    stringBuilder
        .append("Total : ")
        .append(total)
        .append("/")
        .append(total / 1024)
        .append("/")
        .append(total / (1024 * 1024))
        .append("/")
        .append(total / (1024 * 1024 * 1024));
    stringBuilder.append(
        "\r\n-----------------------------------------------------------------------------\r\n");
    return stringBuilder.toString();
  }
}
