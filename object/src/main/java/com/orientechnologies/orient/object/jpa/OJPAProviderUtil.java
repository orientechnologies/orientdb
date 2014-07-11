package com.orientechnologies.orient.object.jpa;

import javax.persistence.spi.LoadState;
import javax.persistence.spi.ProviderUtil;

public class OJPAProviderUtil implements ProviderUtil {

	public LoadState isLoadedWithoutReference(Object entity,
			String attributeName) {
		return LoadState.UNKNOWN;
	}

	public LoadState isLoadedWithReference(Object entity, String attributeName) {
		return LoadState.UNKNOWN;
	}

	public LoadState isLoaded(Object entity) {
		return LoadState.UNKNOWN;
	}

}