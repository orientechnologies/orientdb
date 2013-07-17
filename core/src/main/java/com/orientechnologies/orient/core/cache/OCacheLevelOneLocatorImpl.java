package com.orientechnologies.orient.core.cache;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CACHE_LEVEL1_SIZE;

/**
 * @author Andrey Lomakin
 * @since 05.07.13
 */
public class OCacheLevelOneLocatorImpl implements OCacheLevelOneLocator {
	@Override
	public OCache threadLocalCache() {
		return new ODefaultCache(null, CACHE_LEVEL1_SIZE.getValueAsInteger());
	}
}