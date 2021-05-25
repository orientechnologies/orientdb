package com.orientechnologies.orient.core.storage.index.learned;

import java.util.ArrayList;
import java.util.List;

// FIXME: migrate to MyIndexEngine
public class MyIndex<T> {
  private int numberOfEntries = 0;
  private T firstKey = null;
  private List<Segment> segments = null;
  private List<Long> levelsSizes = null;
  private List<Long> levelsOffset = null;

  /**
   * Constructs the index on the given sorted vector.
   *
   * @param data the vector of keys to be indexed, must be sorted
   */
  public MyIndex(final int epsilon, final List<T> data) {
    // TODO: index construction
    /*for (final T element : data) {
        System.out.println(element);
    }*/

    this.numberOfEntries = data.size();
    this.firstKey = data.get(0);
    int l = numberOfEntries / (epsilon * epsilon);
    this.segments = new ArrayList<>(l);
    this.levelsSizes = new ArrayList<>();
    this.levelsOffset = new ArrayList<>();

    final long epsilonRecursive = 4;
    this.build(
        data, epsilon != 0 ? epsilon : 64, epsilonRecursive, segments, levelsSizes, levelsOffset);
  }

  private void build(
      final List<T> data,
      long epsilon,
      long epsilonRecursive,
      List<Segment> segments,
      List<Long> levelsSizes,
      List<Long> levelsOffset) {
    final long numberOfEntries = data.size();
    if (numberOfEntries == 0) {
      return;
    }
    levelsOffset.add(0L);

    // TODO: check maximum element out of bounds - max is reserved for padding
    // boolean ignoreLast = data.get(data.size()) == Integer.MAX_VALUE;
    // numberOfEntries - Integer.valueOf(ignoreLast);

    buildLevel(epsilon);
  }

  private void buildLevel(final long epsilon) {
    makeSegmentationPar(numberOfEntries, epsilon);
  }

  private int makeSegmentationPar(int numberOfEntries, long epsilon) {
    // TODO: std::min<size_t>(omp_get_max_threads(), 20);
    final int parallelism = 1;
    final int chunkSize = numberOfEntries / parallelism;
    final int c = 0;

    // TODO: if (parallelism == 1 || n < 1ull << 15)
    if (parallelism == 1) {
      return makeSegmentation(numberOfEntries, epsilon);
    }
    return 0;
  }

  private int makeSegmentation(int numberOfEntries, long epsilon) {
    if (numberOfEntries == 0) {
      return 0;
    }
    return 0;
  }

  /**
   * Returns the approximate position and the range where 'value' can be found.
   *
   * @param value the value of the element to search for
   * @return a class with the approximate position and bounds of the range
   */
  public ApproximatePosition search(T value) {
    // TODO: search
    // auto k = std::max(first_key, key);
    // auto it = segment_for_key(k);
    // auto pos = std::min<size_t>((*it)(k), std::next(it)->intercept);
    final int position = 0;
    // auto lo = PGM_SUB_EPS(pos, Epsilon);
    final int low = 0;
    /// auto hi = PGM_ADD_EPS(pos, Epsilon, n);
    final int high = 0;
    // return {pos, lo, hi};
    return new ApproximatePosition(position, low, high);
  }
}
