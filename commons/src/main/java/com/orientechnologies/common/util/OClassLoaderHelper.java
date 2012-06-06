package com.orientechnologies.common.util;

import java.util.Iterator;

import javax.imageio.spi.ServiceRegistry;

public class OClassLoaderHelper {

	/**
	 * Switch to the OrientDb classloader before lookups on ServiceRegistry for
	 * implementation of the given Class. Useful under OSGI and generally under
	 * applications where jars are loaded by another class loader
	 * 
	 * @param clazz
	 *            the class to lookup foor
	 * @return an Iterator on the class implementation
	 */
	public static synchronized <T extends Object> Iterator<T> lookupProviderWithOrientClassLoader(Class<T> clazz) {

		return lookupProviderWithOrientClassLoader(clazz,OClassLoaderHelper.class.getClassLoader());
	}

	public static synchronized <T extends Object> Iterator<T> lookupProviderWithOrientClassLoader(Class<T> clazz,ClassLoader orientClassLoader) {
		
		ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();

		Thread.currentThread().setContextClassLoader(orientClassLoader);
		Iterator<T> lookupProviders = ServiceRegistry.lookupProviders(clazz);
		Thread.currentThread().setContextClassLoader(origClassLoader);

		return lookupProviders;
	}

}
