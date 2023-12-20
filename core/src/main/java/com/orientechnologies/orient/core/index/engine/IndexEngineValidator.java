package com.orientechnologies.orient.core.index.engine;

/**
 * Put operation validator.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
public interface IndexEngineValidator<K, V> {

  /**
   * Indicates that a put request should be silently ignored by the store.
   *
   * @see #validate(Object, Object, Object)
   */
  Object IGNORE = new Object();

  /**
   * Validates the put operation for the given key, the old value and the new value. May throw an
   * exception to abort the current put operation with an error.
   *
   * @param key the put operation key.
   * @param oldValue the old value or {@code null} if no value is currently stored.
   * @param newValue the new value passed to validatedPut(Object, OIdentifiable, Validator).
   * @return the new value to store, may differ from the passed one, or the special {@link #IGNORE}
   *     value to silently ignore the put operation request being processed.
   */
  Object validate(K key, V oldValue, V newValue);
}
