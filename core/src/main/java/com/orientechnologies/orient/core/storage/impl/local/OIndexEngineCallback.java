package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.index.OIndexEngine;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/4/2015
 */
public interface OIndexEngineCallback<T> {
  T callEngine(OIndexEngine engine);
}
