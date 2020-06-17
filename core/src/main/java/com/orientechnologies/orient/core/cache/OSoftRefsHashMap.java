package com.orientechnologies.orient.core.cache;

import com.orientechnologies.common.log.OLogManager;
import java.io.Serializable;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Soft References Map inspired by the code published by Dr. Heinz M. Kabutz on
 * http://www.javaspecialists.eu/archive/Issue015.html.
 */
public class OSoftRefsHashMap<K, V> extends AbstractMap<K, V> implements Serializable {
  private final Map<K, SoftReference<V>> hashCodes = new ConcurrentHashMap<K, SoftReference<V>>();
  private final Map<SoftReference<V>, K> reverseLookup =
      new ConcurrentHashMap<SoftReference<V>, K>();
  private final ReferenceQueue<V> refQueue = new ReferenceQueue<V>();

  public V get(Object key) {
    evictStaleEntries();
    V result = null;
    final SoftReference<V> soft_ref = hashCodes.get(key);
    if (soft_ref != null) {
      result = soft_ref.get();
      if (result == null) {
        hashCodes.remove(key);
        reverseLookup.remove(soft_ref);
      }
    }
    return result;
  }

  private void evictStaleEntries() {
    int evicted = 0;

    Reference<? extends V> sv;
    while ((sv = refQueue.poll()) != null) {
      final K key = reverseLookup.remove(sv);
      if (key != null) {
        hashCodes.remove(key);
        evicted++;
      }
    }

    if (evicted > 0) OLogManager.instance().debug(this, "Evicted %d items", evicted);
  }

  public V put(final K key, final V value) {
    evictStaleEntries();
    final SoftReference<V> soft_ref = new SoftReference<V>(value, refQueue);
    reverseLookup.put(soft_ref, key);
    final SoftReference<V> result = hashCodes.put(key, soft_ref);
    if (result == null) return null;
    reverseLookup.remove(result);
    return result.get();
  }

  public V remove(Object key) {
    evictStaleEntries();
    final SoftReference<V> result = hashCodes.remove(key);
    if (result == null) return null;
    return result.get();
  }

  public void clear() {
    hashCodes.clear();
    reverseLookup.clear();
  }

  public int size() {
    evictStaleEntries();
    return hashCodes.size();
  }

  public Set<Entry<K, V>> entrySet() {
    evictStaleEntries();
    Set<Entry<K, V>> result = new LinkedHashSet<Entry<K, V>>();
    for (final Entry<K, SoftReference<V>> entry : hashCodes.entrySet()) {
      final V value = entry.getValue().get();
      if (value != null) {
        result.add(
            new Entry<K, V>() {
              public K getKey() {
                return entry.getKey();
              }

              public V getValue() {
                return value;
              }

              public V setValue(V v) {
                entry.setValue(new SoftReference<V>(v, refQueue));
                return value;
              }
            });
      }
    }
    return result;
  }
}
