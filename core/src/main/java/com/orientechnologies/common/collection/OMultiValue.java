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
package com.orientechnologies.common.collection;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;

/**
 * Handles Multi-value types such as Arrays, Collections and Maps. It recognizes special Orient collections.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OMultiValue {

  /**
   * Checks if a class is a multi-value type.
   * 
   * @param iType
   *          Class to check
   * @return true if it's an array, a collection or a map, otherwise false
   */
  public static boolean isMultiValue(final Class<?> iType) {
    return OCollection.class.isAssignableFrom(iType)
        || Collection.class.isAssignableFrom(iType)
        || (iType.isArray() || Map.class.isAssignableFrom(iType) || OMultiCollectionIterator.class.isAssignableFrom(iType) || OCollection.class
            .isAssignableFrom(iType));
  }

  /**
   * Checks if the object is a multi-value type.
   * 
   * @param iObject
   *          Object to check
   * @return true if it's an array, a collection or a map, otherwise false
   */
  public static boolean isMultiValue(final Object iObject) {
    return iObject == null ? false : isMultiValue(iObject.getClass());
  }

  public static boolean isIterable(final Object iObject) {
    return iObject == null ? false : iObject instanceof Iterable<?> ? true : iObject instanceof Iterator<?>;
  }

  /**
   * Returns the size of the multi-value object
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @return the size of the multi value object
   */
  public static int getSize(final Object iObject) {
    if (iObject == null)
      return 0;

    if (iObject instanceof OSizeable)
      return ((OSizeable) iObject).size();

    if (!isMultiValue(iObject))
      return 0;

    if (iObject instanceof Collection<?>)
      return ((Collection<Object>) iObject).size();
    if (iObject instanceof Map<?, ?>)
      return ((Map<?, Object>) iObject).size();
    if (iObject.getClass().isArray())
      return Array.getLength(iObject);
    return 0;
  }

  /**
   * Returns the first item of the Multi-value object (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @return The first item if any
   */
  public static Object getFirstValue(final Object iObject) {
    if (iObject == null)
      return null;

    if (!isMultiValue(iObject) || getSize(iObject) == 0)
      return null;

    try {
      if (iObject instanceof List<?>)
        return ((List<Object>) iObject).get(0);
      else if (iObject instanceof Iterable<?>)
        return ((Iterable<Object>) iObject).iterator().next();
      else if (iObject instanceof Map<?, ?>)
        return ((Map<?, Object>) iObject).values().iterator().next();
      else if (iObject.getClass().isArray())
        return Array.get(iObject, 0);
    } catch (Exception e) {
      // IGNORE IT
      OLogManager.instance().debug(iObject, "Error on reading the first item of the Multi-value field '%s'", iObject);
    }

    return null;
  }

  /**
   * Returns the last item of the Multi-value object (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @return The last item if any
   */
  public static Object getLastValue(final Object iObject) {
    if (iObject == null)
      return null;

    if (!isMultiValue(iObject))
      return null;

    try {
      if (iObject instanceof List<?>)
        return ((List<Object>) iObject).get(((List<Object>) iObject).size() - 1);
      else if (iObject instanceof Iterable<?>) {
        Object last = null;
        for (Object o : (Iterable<Object>) iObject)
          last = o;
        return last;
      } else if (iObject instanceof Map<?, ?>) {
        Object last = null;
        for (Object o : ((Map<?, Object>) iObject).values())
          last = o;
        return last;
      } else if (iObject.getClass().isArray())
        return Array.get(iObject, Array.getLength(iObject) - 1);
    } catch (Exception e) {
      // IGNORE IT
      OLogManager.instance().debug(iObject, "Error on reading the last item of the Multi-value field '%s'", iObject);
    }

    return null;
  }

  /**
   * Returns the iIndex item of the Multi-value object (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @param iIndex
   *          integer as the position requested
   * @return The first item if any
   */
  public static Object getValue(final Object iObject, final int iIndex) {
    if (iObject == null)
      return null;

    if (!isMultiValue(iObject))
      return null;

    if (iIndex > getSize(iObject))
      return null;

    try {
      if (iObject instanceof List<?>)
        return ((List<?>) iObject).get(iIndex);
      else if (iObject instanceof Set<?>) {
        int i = 0;
        for (Object o : ((Set<?>) iObject)) {
          if (i++ == iIndex) {
            return o;
          }
        }
      } else if (iObject instanceof Map<?, ?>) {
        int i = 0;
        for (Object o : ((Map<?, ?>) iObject).values()) {
          if (i++ == iIndex) {
            return o;
          }
        }
      } else if (iObject.getClass().isArray())
        return Array.get(iObject, iIndex);
      else if (iObject instanceof Iterator<?> || iObject instanceof Iterable<?>) {

        final Iterator<Object> it = (iObject instanceof Iterable<?>) ? ((Iterable<Object>) iObject).iterator()
            : (Iterator<Object>) iObject;
        for (int i = 0; it.hasNext(); ++i) {
          final Object o = it.next();
          if (i == iIndex) {
            if (it instanceof OResettable)
              ((OResettable) it).reset();

            return o;
          }
        }

        if (it instanceof OResettable)
          ((OResettable) it).reset();
      }
    } catch (Exception e) {
      // IGNORE IT
      OLogManager.instance().debug(iObject, "Error on reading the first item of the Multi-value field '%s'", iObject);
    }
    return null;
  }

  /**
   * Returns an <code>Iterable<Object></code> object to browse the multi-value instance (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   */
  public static Iterable<Object> getMultiValueIterable(final Object iObject) {
    if (iObject == null)
      return null;

    if (iObject instanceof Iterable<?>)
      return (Iterable<Object>) iObject;
    else if (iObject instanceof Collection<?>)
      return ((Collection<Object>) iObject);
    else if (iObject instanceof Map<?, ?>)
      return ((Map<?, Object>) iObject).values();
    else if (iObject.getClass().isArray())
      return new OIterableObjectArray<Object>(iObject);
    else if (iObject instanceof Iterator<?>) {
      final List<Object> temp = new ArrayList<Object>();
      for (Iterator<Object> it = (Iterator<Object>) iObject; it.hasNext();)
        temp.add(it.next());
      return temp;
    }

    return new OIterableObject<Object>(iObject);
  }

  /**
   * Returns an <code>Iterator<Object></code> object to browse the multi-value instance (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   */

  public static Iterator<Object> getMultiValueIterator(final Object iObject) {
    if (iObject == null)
      return null;

    if (iObject instanceof Iterator<?>)
      return (Iterator<Object>) iObject;

    if (iObject instanceof Iterable<?>)
      return ((Iterable<Object>) iObject).iterator();
    if (iObject instanceof Map<?, ?>)
      return ((Map<?, Object>) iObject).values().iterator();
    if (iObject.getClass().isArray())
      return new OIterableObjectArray<Object>(iObject).iterator();

    return new OIterableObject<Object>(iObject);
  }

  /**
   * Returns a stringified version of the multi-value object.
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @return a stringified version of the multi-value object.
   */
  public static String toString(final Object iObject) {
    final StringBuilder sb = new StringBuilder(2048);

    if (iObject instanceof Iterable<?>) {
      final Iterable<Object> coll = (Iterable<Object>) iObject;

      sb.append('[');
      for (final Iterator<Object> it = coll.iterator(); it.hasNext();) {
        try {
          Object e = it.next();
          sb.append(e == iObject ? "(this Collection)" : e);
          if (it.hasNext())
            sb.append(", ");
        } catch (NoSuchElementException ex) {
          // IGNORE THIS
        }
      }
      return sb.append(']').toString();
    } else if (iObject instanceof Map<?, ?>) {
      final Map<String, Object> map = (Map<String, Object>) iObject;

      Entry<String, Object> e;

      sb.append('{');
      for (final Iterator<Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext();) {
        try {
          e = it.next();

          sb.append(e.getKey());
          sb.append(":");
          sb.append(e.getValue() == iObject ? "(this Map)" : e.getValue());
          if (it.hasNext())
            sb.append(", ");
        } catch (NoSuchElementException ex) {
          // IGNORE THIS
        }
      }
      return sb.append('}').toString();
    }

    return iObject.toString();
  }

  /**
   * Utility function that add a value to the main object. It takes care about collections/array and single values.
   * 
   * @param iObject
   *          MultiValue where to add value(s)
   * @param iToAdd
   *          Single value, array of values or collections of values. Map are not supported.
   * @return
   */
  public static Object add(final Object iObject, final Object iToAdd) {
    if (iObject != null) {
      if (!isMultiValue(iObject)) {
        final List<Object> result = new ArrayList<Object>();
        result.add(iObject);
      }

      if (iObject instanceof Collection<?> || iObject instanceof OCollection<?>) {
        // COLLECTION - ?
        final OCollection<Object> coll;
        if (iObject instanceof Collection<?>) {
          final Collection<Object> collection = (Collection<Object>) iObject;
          coll = new OCollection<Object>() {
            @Override
            public void add(Object value) {
              collection.add(value);
            }

            @Override
            public void remove(Object value) {
              collection.remove(value);
            }

            @Override
            public Iterator<Object> iterator() {
              return collection.iterator();
            }

            @Override
            public int size() {
              return collection.size();
            }
          };
        } else
          coll = (OCollection<Object>) iObject;

        if (isMultiValue(iToAdd)) {
          // COLLECTION - COLLECTION
          for (Object o : getMultiValueIterable(iToAdd)) {
            if (isMultiValue(o))
              add(coll, o);
            else
              coll.add(o);
          }
        }

        else if (iToAdd != null && iToAdd.getClass().isArray()) {
          // ARRAY - COLLECTION
          for (int i = 0; i < Array.getLength(iToAdd); ++i) {
            Object o = Array.get(iToAdd, i);
            if (isMultiValue(o))
              add(coll, o);
            else
              coll.add(o);
          }

        } else if (iToAdd instanceof Map<?, ?>) {
          // MAP
          for (Entry<Object, Object> entry : ((Map<Object, Object>) iToAdd).entrySet())
            coll.add(entry.getValue());
        } else if (iToAdd instanceof Iterator<?>) {
          // ITERATOR
          for (Iterator<?> it = (Iterator<?>) iToAdd; it.hasNext();)
            coll.add(it.next());
        } else
          coll.add(iToAdd);

      } else if (iObject.getClass().isArray()) {
        // ARRAY - ?

        final Object[] copy;
        if (iToAdd instanceof Collection<?>) {
          // ARRAY - COLLECTION
          final int tot = Array.getLength(iObject) + ((Collection<Object>) iToAdd).size();
          copy = Arrays.copyOf((Object[]) iObject, tot);
          final Iterator<Object> it = ((Collection<Object>) iToAdd).iterator();
          for (int i = Array.getLength(iObject); i < tot; ++i)
            copy[i] = it.next();

        } else if (iToAdd != null && iToAdd.getClass().isArray()) {
          // ARRAY - ARRAY
          final int tot = Array.getLength(iObject) + Array.getLength(iToAdd);
          copy = Arrays.copyOf((Object[]) iObject, tot);
          System.arraycopy(iToAdd, 0, iObject, Array.getLength(iObject), Array.getLength(iToAdd));

        } else {
          copy = Arrays.copyOf((Object[]) iObject, Array.getLength(iObject) + 1);
          copy[copy.length - 1] = iToAdd;
        }
        return copy;
      }
    }

    return iObject;
  }

  /**
   * Utility function that remove a value from the main object. It takes care about collections/array and single values.
   * 
   * @param iObject
   *          MultiValue where to add value(s)
   * @param iToRemove
   *          Single value, array of values or collections of values. Map are not supported.
   * @param iAllOccurrences
   *          True if the all occurrences must be removed or false of only the first one (Like java.util.Collection.remove())
   * @return
   */
  public static Object remove(Object iObject, Object iToRemove, final boolean iAllOccurrences) {
    if (iObject != null) {
      if (iObject instanceof OMultiCollectionIterator<?>) {
        final Collection<Object> list = new LinkedList<Object>();
        for (Object o : ((OMultiCollectionIterator<?>) iObject))
          list.add(o);
        iObject = list;
      }

      if (iToRemove instanceof OMultiCollectionIterator<?>) {
        // TRANSFORM IN SET ONCE TO OPTIMIZE LOOPS DURING REMOVE
        final Set<Object> set = new HashSet<Object>();
        for (Object o : ((OMultiCollectionIterator<?>) iToRemove))
          set.add(o);
        iToRemove = set;
      }

      if (iObject instanceof Collection<?> || iObject instanceof OCollection<?>) {
        // COLLECTION - ?

        final OCollection<Object> coll;
        if (iObject instanceof Collection<?>) {
          final Collection<Object> collection = (Collection<Object>) iObject;
          coll = new OCollection<Object>() {
            @Override
            public void add(Object value) {
              collection.add(value);
            }

            @Override
            public void remove(Object value) {
              collection.remove(value);
            }

            @Override
            public Iterator<Object> iterator() {
              return collection.iterator();
            }

            @Override
            public int size() {
              return collection.size();
            }
          };
        } else
          coll = (OCollection<Object>) iObject;

        if (iToRemove instanceof Collection<?>) {
          // COLLECTION - COLLECTION
          for (Object o : (Collection<Object>) iToRemove) {
            if (isMultiValue(o))
              remove(coll, o, iAllOccurrences);
            else
              removeFromOCollection(iObject, coll, o, iAllOccurrences);
          }
        }

        else if (iToRemove != null && iToRemove.getClass().isArray()) {
          // ARRAY - COLLECTION
          for (int i = 0; i < Array.getLength(iToRemove); ++i) {
            Object o = Array.get(iToRemove, i);
            if (isMultiValue(o))
              remove(coll, o, iAllOccurrences);
            else
              removeFromOCollection(iObject, coll, o, iAllOccurrences);
          }

        } else if (iToRemove instanceof Map<?, ?>) {
          // MAP
          for (Entry<Object, Object> entry : ((Map<Object, Object>) iToRemove).entrySet())
            coll.remove(entry.getKey());
        } else if (iToRemove instanceof Iterator<?>) {
          // ITERATOR
          if (iToRemove instanceof OMultiCollectionIterator<?>)
            ((OMultiCollectionIterator<?>) iToRemove).reset();

          if (iAllOccurrences) {
            if (iObject instanceof OCollection)
              throw new IllegalStateException("Mutable collection can not be used to remove all occurrences.");

            final Collection<Object> collection = (Collection) iObject;
            OMultiCollectionIterator<?> it = (OMultiCollectionIterator<?>) iToRemove;
            batchRemove(collection, it);
          } else {
            Iterator<?> it = (Iterator<?>) iToRemove;
            if (it.hasNext()) {
              final Object itemToRemove = it.next();
              coll.remove(itemToRemove);
            }
          }
        } else
          removeFromOCollection(iObject, coll, iToRemove, iAllOccurrences);

      } else if (iObject.getClass().isArray()) {
        // ARRAY - ?

        final Object[] copy;
        if (iToRemove instanceof Collection<?>) {
          // ARRAY - COLLECTION
          final int sourceTot = Array.getLength(iObject);
          final int tot = sourceTot - ((Collection<Object>) iToRemove).size();
          copy = new Object[tot];

          int k = 0;
          for (int i = 0; i < sourceTot; ++i) {
            Object o = Array.get(iObject, i);
            if (o != null) {
              boolean found = false;
              for (Object toRemove : (Collection<Object>) iToRemove) {
                if (o.equals(toRemove)) {
                  // SKIP
                  found = true;
                  break;
                }
              }

              if (!found)
                copy[k++] = o;
            }
          }

        } else if (iToRemove != null && iToRemove.getClass().isArray()) {
          throw new UnsupportedOperationException("Cannot execute remove() against an array");

        } else {
          throw new UnsupportedOperationException("Cannot execute remove() against an array");
        }
        return copy;

      } else
        throw new IllegalArgumentException("Object " + iObject + " is not a multi value");
    }

    return iObject;
  }

  protected static void removeFromOCollection(final Object iObject, final OCollection<Object> coll, final Object iToRemove,
      final boolean iAllOccurrences) {
    if (iAllOccurrences && !(iObject instanceof Set)) {
      // BROWSE THE COLLECTION ONE BY ONE TO REMOVE ALL THE OCCURRENCES
      final Iterator<Object> it = coll.iterator();
      while (it.hasNext()) {
        final Object o = it.next();
        if (iToRemove.equals(o))
          it.remove();
      }
    } else
      coll.remove(iToRemove);

  }

  private static void batchRemove(Collection<Object> coll, Iterator<?> it) {
    int approximateRemainingSize;
    if (it instanceof OSizeable) {
      approximateRemainingSize = ((OSizeable) it).size();
    } else {
      approximateRemainingSize = -1;
    }

    while (it.hasNext()) {
      Set<?> batch = prepareBatch(it, approximateRemainingSize);
      coll.removeAll(batch);
      approximateRemainingSize -= batch.size();
    }
  }

  private static Set<?> prepareBatch(Iterator<?> it, int approximateRemainingSize) {
    final HashSet<Object> batch;
    if (approximateRemainingSize > -1) {
      if (approximateRemainingSize > 10000)
        batch = new HashSet<Object>(13400);
      else
        batch = new HashSet<Object>((int) (approximateRemainingSize / 0.75));
    } else {
      batch = new HashSet<Object>();
    }

    int count = 0;
    while (count < 10000 && it.hasNext()) {
      batch.add(it.next());
      count++;
    }

    return batch;
  }

  public static Object[] array(final Object iValue) {
    return array(iValue, Object.class);
  }

  public static <T> T[] array(final Object iValue, final Class<? extends T> iClass) {
    return array(iValue, iClass, null);
  }

  public static <T> T[] array(final Object iValue, final Class<? extends T> iClass, final OCallable<Object, Object> iCallback) {
    if (iValue == null)
      return null;

    final T[] result;

    if (isMultiValue(iValue)) {
      // CREATE STATIC ARRAY AND FILL IT
      result = (T[]) Array.newInstance(iClass, getSize(iValue));
      int i = 0;
      for (Iterator<T> it = (Iterator<T>) getMultiValueIterator(iValue); it.hasNext(); ++i)
        result[i] = (T) convert(it.next(), iCallback);
    } else if (isIterable(iValue)) {
      // SIZE UNKNOWN: USE A LIST AS TEMPORARY OBJECT
      final List<T> temp = new ArrayList<T>();
      for (Iterator<T> it = (Iterator<T>) getMultiValueIterator(iValue); it.hasNext();)
        temp.add((T) convert(it.next(), iCallback));

      if (iClass.equals(Object.class))
        result = (T[]) temp.toArray();
      else
        // CONVERT THEM
        result = temp.toArray((T[]) Array.newInstance(iClass, getSize(iValue)));

    } else {
      result = (T[]) Array.newInstance(iClass, 1);
      result[0] = (T) (T) convert(iValue, iCallback);
    }

    return result;
  }

  public static Object convert(final Object iObject, final OCallable<Object, Object> iCallback) {
    return iCallback != null ? iCallback.call(iObject) : iObject;
  }

  public static boolean equals(final Collection<Object> col1, final Collection<Object> col2) {
    if (col1.size() != col2.size())
      return false;
    return col1.containsAll(col2) && col2.containsAll(col1);
  }

  public static boolean contains(final Object iObject, final Object iItem) {
    if (iObject == null)
      return false;

    if (iObject instanceof Collection)
      return ((Collection) iObject).contains(iItem);

    else if (iObject.getClass().isArray()) {
      final int size = Array.getLength(iObject);
      for (int i = 0; i < size; ++i) {
        final Object item = Array.get(iObject, i);
        if (item != null && item.equals(iItem))
          return true;
      }
    }

    return false;
  }

  public static int indexOf(final Object iObject, final Object iItem) {
    if (iObject == null)
      return -1;

    if (iObject instanceof List)
      return ((List) iObject).indexOf(iItem);

    else if (iObject.getClass().isArray()) {
      final int size = Array.getLength(iObject);
      for (int i = 0; i < size; ++i) {
        final Object item = Array.get(iObject, i);
        if (item != null && item.equals(iItem))
          return i;
      }
    }

    return -1;
  }
}
