package com.orientechnologies.orient.core.storage.impl.utils.linearhashing;

import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.id.OClusterPosition;

/**
 * @author Artem Loginov (logart2007@gmail.com)
 */
public class OLinearHashingHashCalculator {
  public static final OLinearHashingHashCalculator      INSTANCE          = new OLinearHashingHashCalculator();

  private static final ThreadLocal<MersenneTwisterFast> threadLocalRandom = new ThreadLocal<MersenneTwisterFast>() {
                                                                            @Override
                                                                            protected MersenneTwisterFast initialValue() {
                                                                              return new MersenneTwisterFast();
                                                                            }
                                                                          };

  public long calculateNaturalOrderedHash(OClusterPosition key, long level) {
    return key.longValueHigh() >> (63 - level);
  }

  public int calculateBucketNumber(long hash, long level) {
    final int result;
    if (level == 0 && hash == 0)
      return 0;
    if (((hash & 1) == 0) && (level > 0))
      return calculateBucketNumber(hash / 2, level - 1);
    else
      result = (int) ((hash - 1) / 2 + (1 << (level - 1)));
    assert result >= 0;
    return result;
  }

  public byte calculateSignature(OClusterPosition key) {
    final MersenneTwisterFast random = threadLocalRandom.get();

    random.setSeed(key.longValue());
    return (byte) (random.nextInt() & 0xFF);
  }
}
