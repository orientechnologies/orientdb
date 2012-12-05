package com.orientechnologies.orient.core.storage.impl.memory.lh;

import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
abstract class OLinearHashingHashCalculatorFactory {

  public static final OLinearHashingHashCalculatorFactory INSTANCE;
  private static final ThreadLocal<MersenneTwisterFast> threadLocalRandom = new ThreadLocal();

  static {
    if (OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.getValueAsBoolean())
      INSTANCE = new OLinearHashingHashCalculatorNodeId();
    else
      INSTANCE = new OLinearHashingHashCalculatorLong();
  }

  public int calculateNaturalOrderedHash(OClusterPosition key, int level) {
    return (int) Math.floor(Math.pow(2, level) * calculateHashIn01Range(key, level));
  }

  public int calculateBucketNumber(int hash, int level) {
    final int result;
    if (level == 0 && hash == 0)
      return 0;
    if ((hash % 2 == 0) && (level > 0))
      return calculateBucketNumber(hash / 2, level - 1);
    else
      result = (hash - 1) / 2 + (int) Math.pow(2, level - 1);
    assert result >= 0;
    return result;
  }

  public double calculateHashIn01Range(OClusterPosition key, int level) {
    assert key != null;
    double result = ((key.doubleValue() + (getNegativeElementCount())) / getElementCount());

    // TODO remove this hack and use same valid workaround
    if (result >= 1) {
      result = 0.999999999999999;
    }
    assert result >= 0;
    assert result < 1;
    return result;
  }

  protected abstract double getElementCount();

  protected abstract double getNegativeElementCount();

  public byte calculateSignature(OClusterPosition key) {

    if(threadLocalRandom.get() == null){
      threadLocalRandom.set(new MersenneTwisterFast());
    }

    MersenneTwisterFast random = threadLocalRandom.get();

    random.setSeed(key.longValue());
    return (byte) (random.nextInt() & 0xFF);
  }

  private static final class OLinearHashingHashCalculatorLong extends OLinearHashingHashCalculatorFactory {
    private static final double ELEMENTS_COUNT = Math.pow(2, 64);

    @Override
    protected double getElementCount() {
      return ELEMENTS_COUNT;
    }

    @Override
    protected double getNegativeElementCount() {
      return -(double) Long.MIN_VALUE;
    }

  }

  private static final class OLinearHashingHashCalculatorNodeId extends OLinearHashingHashCalculatorFactory {
    private static final double ELEMENTS_COUNT         = Math.pow(2, 192);
    private static final double HALF_OF_ELEMENTS_COUNT = ELEMENTS_COUNT / 2;

    @Override
    protected double getElementCount() {
      return ELEMENTS_COUNT;
    }

    @Override
    protected double getNegativeElementCount() {
      return HALF_OF_ELEMENTS_COUNT;
    }

  }
}
