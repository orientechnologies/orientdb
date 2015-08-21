/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.common.reflection;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.orientechnologies.common.log.OLogManager;

/**
 * Helper class to browse .class files. See also: http://forums.sun.com/thread.jspa?threadID=341935&start=15&tstart=0
 * 
 * @author Antony Stubbs
 */
public class OReflectionHelper {
  private static final String CLASS_EXTENSION = ".class";

  public static List<Class<?>> getClassesFor(final Collection<String> classNames, final ClassLoader classLoader)
      throws ClassNotFoundException {
    List<Class<?>> classes = new ArrayList<Class<?>>(classNames.size());
    for (String className : classNames) {
      classes.add(Class.forName(className, true, classLoader));
    }
    return classes;
  }

  public static List<Class<?>> getClassesFor(final String iPackageName, final ClassLoader iClassLoader)
      throws ClassNotFoundException {
    // This will hold a list of directories matching the pckgname.
    // There may be more than one if a package is split over multiple jars/paths
    final List<Class<?>> classes = new ArrayList<Class<?>>();
    final ArrayList<File> directories = new ArrayList<File>();
    try {
      // Ask for all resources for the path
      final String packageUrl = iPackageName.replace('.', '/');
      Enumeration<URL> resources = iClassLoader.getResources(packageUrl);
      if (!resources.hasMoreElements()) {
        resources = iClassLoader.getResources(packageUrl + CLASS_EXTENSION);
        if (resources.hasMoreElements()) {
          throw new IllegalArgumentException(iPackageName + " does not appear to be a valid package but a class");
        }
      } else {
        while (resources.hasMoreElements()) {
          final URL res = resources.nextElement();
          if (res.getProtocol().equalsIgnoreCase("jar")) {
            final JarURLConnection conn = (JarURLConnection) res.openConnection();
            final JarFile jar = conn.getJarFile();
            for (JarEntry e : Collections.list(jar.entries())) {

              if (e.getName().startsWith(iPackageName.replace('.', '/')) && e.getName().endsWith(CLASS_EXTENSION)
                  && !e.getName().contains("$")) {
                final String className = e.getName().replace("/", ".").substring(0, e.getName().length() - 6);
                classes.add(Class.forName(className, true, iClassLoader));
              }
            }
          } else
            directories.add(new File(URLDecoder.decode(res.getPath(), "UTF-8")));
        }
      }
    } catch (NullPointerException x) {
      throw new ClassNotFoundException(iPackageName + " does not appear to be " + "a valid package (Null pointer exception)", x);
    } catch (UnsupportedEncodingException encex) {
      throw new ClassNotFoundException(iPackageName + " does not appear to be " + "a valid package (Unsupported encoding)", encex);
    } catch (IOException ioex) {
      throw new ClassNotFoundException("IOException was thrown when trying " + "to get all resources for " + iPackageName, ioex);
    }

    // For every directory identified capture all the .class files
    for (File directory : directories) {
      if (directory.exists()) {
        // Get the list of the files contained in the package
        File[] files = directory.listFiles();
        for (File file : files) {
          if (file.isDirectory()) {
            classes.addAll(findClasses(file, iPackageName, iClassLoader));
          } else {
            String className;
            if (file.getName().endsWith(CLASS_EXTENSION)) {
              className = file.getName().substring(0, file.getName().length() - CLASS_EXTENSION.length());
              classes.add(Class.forName(iPackageName + '.' + className, true, iClassLoader));
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
  private static List<Class<?>> findClasses(final File iDirectory, String iPackageName, ClassLoader iClassLoader)
      throws ClassNotFoundException {
    final List<Class<?>> classes = new ArrayList<Class<?>>();
    if (!iDirectory.exists())
      return classes;

    iPackageName += "." + iDirectory.getName();

    String className;
    final File[] files = iDirectory.listFiles();
    if (files != null)
      for (File file : files) {
        if (file.isDirectory()) {
          if (file.getName().contains("."))
            continue;
          classes.addAll(findClasses(file, iPackageName, iClassLoader));
        } else if (file.getName().endsWith(CLASS_EXTENSION)) {
          className = file.getName().substring(0, file.getName().length() - CLASS_EXTENSION.length());
          classes.add(Class.forName(iPackageName + '.' + className, true, iClassLoader));
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
      for (Class<?> discovered : getClassesFor(thePackage, iClassLoader)) {
        if (Arrays.asList(discovered.getInterfaces()).contains(theInterface)) {
          classList.add(discovered);
        }
      }
    } catch (ClassNotFoundException ex) {
      OLogManager.instance().error(null, "Error finding classes", ex);
    }

    return classList;
  }

  /**
   * Returns the declared generic types of a class.
   * 
   * @param iClass
   *          Class to examine
   * @return The array of Type if any, otherwise null
   */
  public static Type[] getGenericTypes(final Class<?> iClass) {
    final Type genericType = iClass.getGenericInterfaces()[0];
    if (genericType != null && genericType instanceof ParameterizedType) {
      final ParameterizedType pt = (ParameterizedType) genericType;
      if (pt.getActualTypeArguments() != null && pt.getActualTypeArguments().length > 1)
        return pt.getActualTypeArguments();
    }
    return null;
  }

  /**
   * Returns the generic class of multi-value objects.
   * 
   * @param p
   *          Field to examine
   * @return The Class<?> of generic type if any, otherwise null
   */
  public static Class<?> getGenericMultivalueType(final Field p) {
    if (p.getType() instanceof Class<?>) {
      final Type genericType = p.getGenericType();
      if (genericType != null && genericType instanceof ParameterizedType) {
        final ParameterizedType pt = (ParameterizedType) genericType;
        if (pt.getActualTypeArguments() != null && pt.getActualTypeArguments().length > 0) {
          if (((Class<?>) pt.getRawType()).isAssignableFrom(Map.class)) {
            if (pt.getActualTypeArguments()[1] instanceof Class<?>) {
              return (Class<?>) pt.getActualTypeArguments()[1];
            } else if (pt.getActualTypeArguments()[1] instanceof ParameterizedType)
              return (Class<?>) ((ParameterizedType) pt.getActualTypeArguments()[1]).getRawType();
          } else if (pt.getActualTypeArguments()[0] instanceof Class<?>) {
            return (Class<?>) pt.getActualTypeArguments()[0];
          } else if (pt.getActualTypeArguments()[0] instanceof ParameterizedType)
            return (Class<?>) ((ParameterizedType) pt.getActualTypeArguments()[0]).getRawType();
        }
      } else if (p.getType().isArray())
        return p.getType().getComponentType();
    }
    return null;
  }

  /**
   * Checks if a class is a Java type: Map, Collection,arrays, Number (extensions and primitives), String, Boolean..
   * 
   * @param clazz
   *          Class<?> to examine
   * @return true if clazz is Java type, false otherwise
   */
  public static boolean isJavaType(Class<?> clazz) {
    if (clazz.isPrimitive())
      return true;
    else if (clazz.getName().startsWith("java.lang"))
      return true;
    else if (clazz.getName().startsWith("java.util"))
      return true;
    else if (clazz.isArray())
      return true;
    return false;
  }
}
