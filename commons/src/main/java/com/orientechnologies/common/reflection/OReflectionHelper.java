package com.orientechnologies.common.reflection;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.orientechnologies.common.log.OLogManager;

/**
 * Helper class to browse .class files. See also: http://forums.sun.com/thread.jspa?threadID=341935&start=15&tstart=0
 * 
 * @author Antony Stubbs
 */
public class OReflectionHelper {
	private static final String	CLASS_EXTENSION	= ".class";

	public static List<Class<?>> getClassesForPackage(final String iPackageName, final ClassLoader iClassLoader)
			throws ClassNotFoundException {
		// This will hold a list of directories matching the pckgname.
		// There may be more than one if a package is split over multiple jars/paths
		List<Class<?>> classes = new ArrayList<Class<?>>();
		ArrayList<File> directories = new ArrayList<File>();
		try {
			// Ask for all resources for the path
			Enumeration<URL> resources = iClassLoader.getResources(iPackageName.replace('.', '/'));
			while (resources.hasMoreElements()) {
				URL res = resources.nextElement();
				if (res.getProtocol().equalsIgnoreCase("jar")) {
					JarURLConnection conn = (JarURLConnection) res.openConnection();
					JarFile jar = conn.getJarFile();
					for (JarEntry e : Collections.list(jar.entries())) {

						if (e.getName().startsWith(iPackageName.replace('.', '/')) && e.getName().endsWith(CLASS_EXTENSION)
								&& !e.getName().contains("$")) {
							String className = e.getName().replace("/", ".").substring(0, e.getName().length() - 6);
							classes.add(Class.forName(className));
						}
					}
				} else
					directories.add(new File(URLDecoder.decode(res.getPath(), "UTF-8")));
			}
		} catch (NullPointerException x) {
			throw new ClassNotFoundException(iPackageName + " does not appear to be " + "a valid package (Null pointer exception)");
		} catch (UnsupportedEncodingException encex) {
			throw new ClassNotFoundException(iPackageName + " does not appear to be " + "a valid package (Unsupported encoding)");
		} catch (IOException ioex) {
			throw new ClassNotFoundException("IOException was thrown when trying " + "to get all resources for " + iPackageName);
		}

		// For every directory identified capture all the .class files
		for (File directory : directories) {
			if (directory.exists()) {
				// Get the list of the files contained in the package
				File[] files = directory.listFiles();
				for (File file : files) {
					if (file.isDirectory()) {
						classes.addAll(findClasses(file, iPackageName));
					} else {
						String className;
						if (file.getName().endsWith(CLASS_EXTENSION)) {
							className = file.getName().substring(0, file.getName().length() - CLASS_EXTENSION.length());
							classes.add(Class.forName(iPackageName + '.' + className));
						}
					}
				}
			} else {
				throw new ClassNotFoundException(iPackageName + " (" + directory.getPath() + ") does not appear to be a valid package");
			}
		}
		return classes;
	}

	/**
	 * Recursive method used to find all classes in a given directory and subdirs.
	 * 
	 * @param iDirectory
	 *          The base directory
	 * @param iPackageName
	 *          The package name for classes found inside the base directory
	 * @return The classes
	 * @throws ClassNotFoundException
	 */
	private static List<Class<?>> findClasses(final File iDirectory, String iPackageName) throws ClassNotFoundException {
		final List<Class<?>> classes = new ArrayList<Class<?>>();
		if (!iDirectory.exists())
			return classes;

		iPackageName += "." + iDirectory.getName();

		String className;
		final File[] files = iDirectory.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				if (file.getName().contains("."))
					continue;
				classes.addAll(findClasses(file, iPackageName));
			} else if (file.getName().endsWith(CLASS_EXTENSION)) {
				className = file.getName().substring(0, file.getName().length() - CLASS_EXTENSION.length());
				classes.add(Class.forName(iPackageName + '.' + className));
			}
		}
		return classes;
	}

	/**
	 * Filters discovered classes to see if they implement a given interface.
	 * 
	 * @param thePackage
	 * @param theInterface
	 * @param iClassLoader
	 * @return The list of classes that implements the requested interface
	 */
	public static List<Class<?>> getClassessOfInterface(String thePackage, Class<?> theInterface, final ClassLoader iClassLoader) {
		List<Class<?>> classList = new ArrayList<Class<?>>();
		try {
			for (Class<?> discovered : getClassesForPackage(thePackage, iClassLoader)) {
				if (Arrays.asList(discovered.getInterfaces()).contains(theInterface)) {
					classList.add(discovered);
				}
			}
		} catch (ClassNotFoundException ex) {
			OLogManager.instance().error(null, "Error finding classes", ex);
		}

		return classList;
	}
}