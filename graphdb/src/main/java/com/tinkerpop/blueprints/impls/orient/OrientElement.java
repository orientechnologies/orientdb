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

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OStorage;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Base Graph Element where OrientVertex and OrientEdge classes extends from. Labels are managed as OrientDB classes.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public abstract class OrientElement implements Element, OSerializableStream, Externalizable, OIdentifiable {
  public static final String                   LABEL_FIELD_NAME          = "label";
  public static final Object                   DEF_ORIGINAL_ID_FIELDNAME = "origId";
  private static final long                    serialVersionUID          = 1L;
  protected boolean                            classicDetachMode         = false;
  protected transient OrientBaseGraph.Settings settings;
  protected OIdentifiable                      rawElement;
  private transient OrientBaseGraph            graph;

  protected OrientElement(final OrientBaseGraph rawGraph, final OIdentifiable iRawElement) {
    if (classicDetachMode)
      graph = rawGraph;
    else
      graph = null;

    if (graph == null)
      graph = getGraph();

    if (graph != null)
      settings = graph.settings;

    rawElement = iRawElement;
  }

  public abstract String getLabel();

  public abstract String getBaseClassName();

  /**
   * (Blueprints Extension) Returns the element type in form of String between "Vertex" and "Edge".
   */
  public abstract String getElementType();

  /**
   * Removes the Element from the Graph. In case the element is a Vertex, all the incoming and outgoing edges are automatically
   * removed too.
   */
  void removeRecord() {
    checkIfAttached();

    final OrientBaseGraph graph = getGraph();
    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();

    if (checkDeletedInTx())
      graph.throwRecordNotFoundException("The graph element with id '" + getIdentity() + "' not found");

    try {
      getRecord().load();
    } catch (ORecordNotFoundException e) {
      graph.throwRecordNotFoundException(e.getMessage());
    }
    getRecord().delete();
  }

  protected boolean checkDeletedInTx() {
    OrientBaseGraph curGraph = getGraph();
    if (curGraph == null)
      return false;

    ORID id;
    if (getRecord() != null)
      id = getRecord().getIdentity();
    else
      return false;

    final ORecordOperation oper = curGraph.getRawGraph().getTransaction().getRecordEntry(id);
    if (oper == null)
      return id.isTemporary();
    else
      return oper.type == ORecordOperation.DELETED;
  }

  /**
   * (Blueprints Extension) Sets multiple properties in one shot against Vertices and Edges. This improves performance avoiding to
   * save the graph element at every property set.<br>
   * Example:
   * 
   * <code>
   * vertex.setProperties( "name", "Jill", "age", 33, "city", "Rome", "born", "Victoria, TX" );
   * </code> You can also pass a Map of values as first argument. In this case all the map entries will be set as element
   * properties:
   * 
   * <code>
   * Map<String,Object> props = new HashMap<String,Object>();
   * props.put("name", "Jill");
   * props.put("age", 33);
   * props.put("city", "Rome");
   * props.put("born", "Victoria, TX");
   * vertex.setProperties(props);
   * </code>
   * 
   * @param fields
   *          Odd number of fields to set as repeating pairs of key, value, or if one parameter is received and it's a Map, the Map
   *          entries are used as field key/value pairs.
   * @param <T>
   * @return
   */
  public <T extends OrientElement> T setProperties(final Object... fields) {
    if (checkDeletedInTx())
      graph.throwRecordNotFoundException("The graph element " + getIdentity() + " has been deleted");

    setPropertiesInternal(fields);
    save();
    return (T) this;
  }

  /**
   * (Blueprints Extension) Gets all the properties from a Vertex or Edge in one shot.
   * 
   * @return a map containing all the properties of the Vertex/Edge.
   */
  public Map<String, Object> getProperties() {
    if (this.rawElement == null)
      return null;
    ODocument raw = this.rawElement.getRecord();
    if (raw == null)
      return null;
    return raw.toMap();
  }

  /**
   * Sets a Property value.
   * 
   * @param key
   *          Property name
   * @param value
   *          Property value
   */
  @Override
  public void setProperty(final String key, final Object value) {
    if (checkDeletedInTx())
      graph.throwRecordNotFoundException("The graph element " + getIdentity() + " has been deleted");

    validateProperty(this, key, value);
    final OrientBaseGraph graph = getGraph();

    if (graph != null)
      graph.autoStartTransaction();
    getRecord().field(key, value);
    if (graph != null)
      save();
  }

  /**
   * Sets a Property value specifying a type. This is useful when you don't have a schema on this property but you want to force the
   * type.
   *
   * @param key
   *          Property name
   * @param value
   *          Property value
   * @param iType
   *          Type to set
   */
  public void setProperty(final String key, final Object value, final OType iType) {
    if (checkDeletedInTx())
      graph.throwRecordNotFoundException("The graph element " + getIdentity() + " has been deleted");

    validateProperty(this, key, value);

    final OrientBaseGraph graph = getGraph();
    if (graph != null)
      graph.autoStartTransaction();
    getRecord().field(key, value, iType);
    if (graph != null)
      save();
  }

  /**
   * Removes a Property.
   * 
   * @param key
   *          Property name
   * @return Old value if any
   */
  @Override
  public <T> T removeProperty(final String key) {
    if (checkDeletedInTx())
      throw new IllegalStateException("The vertex " + getIdentity() + " has been deleted");

    final OrientBaseGraph graph = getGraph();

    if (graph != null)
      graph.autoStartTransaction();

    final Object oldValue = getRecord().removeField(key);
    if (graph != null)
      save();
    return (T) oldValue;
  }

  /**
   * Returns a Property value.
   * 
   * @param key
   *          Property name
   * @return Property value if any, otherwise NULL.
   */
  @Override
  public <T> T getProperty(final String key) {
    if (key == null)
      return null;

    final OrientBaseGraph graph = getGraph();
    if (key.equals("_class"))
      return (T) ODocumentInternal.getImmutableSchemaClass(getRecord()).getName();
    else if (key.equals("_version"))
      return (T) new Integer(getRecord().getVersion());
    else if (key.equals("_rid"))
      return (T) rawElement.getIdentity().toString();

    final Object fieldValue = getRecord().field(key);
    if (graph != null && fieldValue instanceof OIdentifiable && !(((OIdentifiable) fieldValue).getRecord() instanceof ORecordBytes))
      // CONVERT IT TO VERTEX/EDGE
      return (T) graph.getElement(fieldValue);
    else if (OMultiValue.isMultiValue(fieldValue) && OMultiValue.getFirstValue(fieldValue) instanceof OIdentifiable) {
      final OIdentifiable firstValue = (OIdentifiable) OMultiValue.getFirstValue(fieldValue);

      if (firstValue instanceof ODocument) {
        final ODocument document = (ODocument) firstValue;

        if (document.isEmbedded() || ODocumentInternal.getImmutableSchemaClass(document) == null)
          return (T) fieldValue;
      }

      if (graph != null)
        // CONVERT IT TO ITERABLE<VERTEX/EDGE>
        return (T) new OrientElementIterable<OrientElement>(graph, OMultiValue.getMultiValueIterable(fieldValue));
    }

    return (T) fieldValue;
  }

  /**
   * Returns the Element Id assuring to save it if it's transient yet.
   */
  @Override
  public Object getId() {
    return getIdentity();
  }

  /**
   * (Blueprints Extension) Saves current element. You don't need to call save() unless you're working against Temporary Vertices.
   */
  public void save() {
    save(null);
  }

  /**
   * (Blueprints Extension) Saves current element to a particular cluster. You don't need to call save() unless you're working
   * against Temporary Vertices.
   * 
   * @param iClusterName
   *          Cluster name or null to use the default "E"
   */
  public void save(final String iClusterName) {
    checkIfAttached();

    final OrientBaseGraph graph = getGraph();
    graph.setCurrentGraphInThreadLocal();

    if (rawElement instanceof ODocument)
      if (iClusterName != null)
        rawElement = ((ODocument) rawElement).save(iClusterName);
      else
        rawElement = ((ODocument) rawElement).save();
  }

  public int hashCode() {
    return ((rawElement == null) ? 0 : rawElement.hashCode());
  }

  /**
   * (Blueprints Extension) Serializes the Element as byte[]
   * 
   * @throws OSerializationException
   */
  @Override
  public byte[] toStream() throws OSerializationException {
    return rawElement.getIdentity().toString().getBytes();
  }

  /**
   * (Blueprints Extension) Fills the Element from a byte[]
   * 
   * @param stream
   *          byte array representation of the object
   * @throws OSerializationException
   */
  @Override
  public OSerializableStream fromStream(final byte[] stream) throws OSerializationException {
    final ODocument record = getRecord();
    ((ORecordId) record.getIdentity()).fromString(new String(stream));
    return this;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeObject(rawElement != null ? rawElement.getIdentity() : null);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    rawElement = (OIdentifiable) in.readObject();
  }

  /**
   * (Blueprints Extension) Locks current Element to prevent concurrent access. If lock is exclusive, then no concurrent threads can
   * read/write it. If the lock is shared, then concurrent threads can only read Element properties, but can't change them. Locks
   * can be freed by calling @unlock or when the current transaction is closed (committed or rollbacked).
   * 
   * @see #lock(boolean)
   * @param iExclusive
   *          True = Exclusive Lock, False = Shared Lock
   */
  @Override
  public void lock(final boolean iExclusive) {
    ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().lockRecord(this,
        iExclusive ? OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK : OStorage.LOCKING_STRATEGY.SHARED_LOCK);
  }

  /**
   * (Blueprints Extension) Checks if an Element is locked
   */
  @Override
  public boolean isLocked() {
    return ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().isLockedRecord(this);
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    return ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().lockingStrategy(this);
  }

  /**
   * (Blueprints Extension) Unlocks previous acquired @lock against the Element.
   * 
   * @see #lock(boolean)
   */
  @Override
  public void unlock() {
    ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().unlockRecord(this);
  }

  /**
   * (Blueprints Extension) Returns the record's identity.
   */
  @Override
  public ORID getIdentity() {
    if (rawElement == null)
      return ORecordId.EMPTY_RECORD_ID;

    final ORID rid = rawElement.getIdentity();
    final OrientBaseGraph graph = getGraph();

    if (!rid.isValid() && graph != null) {
      // SAVE THE RECORD TO OBTAIN A VALID RID
      graph.setCurrentGraphInThreadLocal();
      graph.autoStartTransaction();
      save();
    }
    return rid;
  }

  /**
   * (Blueprints Extension) Returns the underlying record.
   */
  @Override
  public ODocument getRecord() {
    if (rawElement == null)
      return null;

    if (rawElement instanceof ODocument)
      return (ODocument) rawElement;

    final ODocument doc = rawElement.getRecord();
    if (doc == null)
      return null;

    // CHANGE THE RID -> DOCUMENT
    rawElement = doc;
    return doc;
  }

  /**
   * (Blueprints Extension) Removes the reference to the current graph instance to let working offline. To reattach it use @attach.
   *
   * This methods works only in "classic detach/attach mode" when dettachment/attachment is done manually, by default it is done
   * automatically, and currently active graph connection will be used as graph elements owner.
   *
   * @return Current object to allow chained calls.
   * @see #attach(OrientBaseGraph), #isDetached
   */
  public OrientElement detach() {
    // EARLY UNMARSHALL FIELDS
    getRecord().setLazyLoad(false);
    getRecord().fieldNames();
    // COPY GRAPH SETTINGS TO WORK OFFLINE
    if (graph != null) {
      settings = graph.settings.copy();
      graph = null;
    }
    classicDetachMode = true;
    return this;
  }

  /**
   * Switches to auto attachment mode, when graph element is automatically attached to currently open graph instance.
   */
  public void switchToAutoAttachmentMode() {
    graph = null;
    classicDetachMode = false;
  }

  /**
   * Behavior is the same as for {@link #attach(OrientBaseGraph)} method.
   */
  public void switchToManualAttachmentMode(final OrientBaseGraph iNewGraph) {
    attach(iNewGraph);
  }

  /**
   * (Blueprints Extension) Replaces current graph instance with new one on @detach -ed elements. Use this method to pass elements
   * between graphs or to switch between Tx and NoTx instances.
   * 
   * This methods works only in "classic detach/attach mode" when detachment/attachment is done manually, by default it is done
   * automatically, and currently active graph connection will be used as graph elements owner.
   *
   * To set "classic detach/attach mode" please set custom database parameter <code>classicDetachMode</code> to <code>true</code>.
   * 
   * @param iNewGraph
   *          The new Graph instance to use.
   * @return Current object to allow chained calls.
   * @see #detach(), #isDetached
   */
  public OrientElement attach(final OrientBaseGraph iNewGraph) {
    if (iNewGraph == null)
      throw new IllegalArgumentException("Graph is null");

    classicDetachMode = true;
    graph = iNewGraph;

    // LINK THE GRAPHS SETTINGS
    settings = graph.settings;
    return this;
  }

  /**
   * (Blueprints Extension) Tells if the current element has been @detach ed.
   *
   * This methods works only in "classic detach/attach mode" when detachment/attachment is done manually, by default it is done
   * automatically, and currently active graph connection will be used as graph elements owner.
   * 
   * To set "classic detach/attach mode" please set custom database parameter <code>classicDetachMode</code> to <code>true</code>.
   *
   * @return True if detached, otherwise false
   * @see #attach(OrientBaseGraph), #detach
   */
  public boolean isDetached() {
    return getGraph() == null;
  }

  public boolean equals(final Object object) {
    return ElementHelper.areEqual(this, object);
  }

  public int compare(final OIdentifiable iFirst, final OIdentifiable iSecond) {
    if (iFirst == null || iSecond == null)
      return -1;
    return iFirst.compareTo(iSecond);
  }

  public int compareTo(final OIdentifiable iOther) {
    if (iOther == null)
      return 1;

    final ORID myRID = getIdentity();
    final ORID otherRID = iOther.getIdentity();

    if (myRID == null && otherRID == null)
      return 0;
    if (myRID == null)
      return -1;
    if (otherRID == null)
      return 1;

    return myRID.compareTo(otherRID);
  }

  /**
   * (Blueprints Extension) Returns the Graph instance associated to the current element. On @detach ed elements returns NULL.
   * 
   */
  public OrientBaseGraph getGraph() {
    if (classicDetachMode)
      return graph;

    final OrientBaseGraph g = OrientBaseGraph.getActiveGraph();

    if (graph != null && (g == null || !g.getRawGraph().getName().equals(graph.getRawGraph().getName()))) {
      // INVALID GRAPH INSTANCE IN TL, SET CURRENT ONE
      OrientBaseGraph.clearInitStack();
      graph.makeActive();
      return this.graph;
    }

    return g;
  }

  /**
   * (Blueprints Extension) Validates an Element property.
   * 
   * @param element
   *          Element instance
   * @param key
   *          Property name
   * @param value
   *          property value
   * @throws IllegalArgumentException
   */
  public final void validateProperty(final Element element, final String key, final Object value) throws IllegalArgumentException {
    if (settings.isStandardElementConstraints() && null == value)
      throw ExceptionFactory.propertyValueCanNotBeNull();
    if (null == key)
      throw ExceptionFactory.propertyKeyCanNotBeNull();
    if (settings.isStandardElementConstraints() && key.equals(StringFactory.ID))
      throw ExceptionFactory.propertyKeyIdIsReserved();
    if (element instanceof Edge && key.equals(StringFactory.LABEL))
      throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
    if (key.isEmpty())
      throw ExceptionFactory.propertyKeyCanNotBeEmpty();
  }

  public void reload() {
    final ODocument rec = getRecord();
    if (rec != null)
      rec.reload(null, true);
  }

  protected void copyTo(final OrientElement iCopy) {
    iCopy.graph = graph;
    iCopy.settings = settings;
    if (rawElement instanceof ODocument) {
      iCopy.rawElement = new ODocument().fromStream(((ODocument) rawElement).toStream());
    } else if (rawElement instanceof ORID)
      iCopy.rawElement = ((ORID) rawElement).copy();
    else
      throw new IllegalArgumentException("Cannot clone element " + rawElement);
  }

  protected void checkClass() {
    // FORCE EARLY UNMARSHALLING
    final ODocument doc = getRecord();
    doc.deserializeFields();

    final OClass cls = ODocumentInternal.getImmutableSchemaClass(doc);

    if (cls == null || !cls.isSubClassOf(getBaseClassName()))
      throw new IllegalArgumentException("The document received is not a " + getElementType() + ". Found class '" + cls + "'");
  }

  /**
   * Check if a class already exists, otherwise create it at the fly. If a transaction is running commit changes, create the class
   * and begin a new transaction.
   * 
   * @param className
   *          Class's name
   */
  protected String checkForClassInSchema(final String className) {
    if (className == null)
      return null;

    final OrientBaseGraph graph = getGraph();
    if (graph == null)
      return className;

    final OSchema schema = graph.getRawGraph().getMetadata().getSchema();

    if (!schema.existsClass(className)) {
      // CREATE A NEW CLASS AT THE FLY
      try {
        graph.executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {

          @Override
          public OClass call(final OrientBaseGraph g) {
            return schema.createClass(className, schema.getClass(getBaseClassName()));

          }
        }, "Committing the active transaction to create the new type '", className, "' as subclass of '", getBaseClassName(),
            "'. The transaction will be reopen right after that. To avoid this behavior create the classes outside the transaction");

      } catch (OSchemaException e) {
        if (!schema.existsClass(className))
          throw e;
      }
    } else {
      // CHECK THE CLASS INHERITANCE
      final OClass cls = schema.getClass(className);
      if (!cls.isSubClassOf(getBaseClassName()))
        throw new IllegalArgumentException("Class '" + className + "' is not an instance of " + getBaseClassName());
    }

    return className;
  }

  protected void setPropertyInternal(final Element element, final ODocument doc, final String key, final Object value) {
    validateProperty(element, key, value);
    doc.field(key, value);
  }

  protected OrientBaseGraph setCurrentGraphInThreadLocal() {
    final OrientBaseGraph graph = getGraph();
    if (graph != null)
      graph.setCurrentGraphInThreadLocal();
    return graph;
  }

  protected OrientBaseGraph checkIfAttached() {
    final OrientBaseGraph graph = getGraph();
    if (graph == null)
      throw new IllegalStateException(
          "There is no active graph instance for current element. Please either open connection to your storage, or use detach/attach methods instead.");
    return graph;
  }

  /**
   * (Blueprints Extension) Sets multiple properties in one shot against Vertices and Edges without saving the element. This
   * improves performance avoiding to save the graph element at every property set. Example:
   *
   * <code>
   * vertex.setProperties( "name", "Jill", "age", 33, "city", "Rome", "born", "Victoria, TX" );
   * </code> You can also pass a Map of values as first argument. In this case all the map entries will be set as element
   * properties:
   *
   * <code>
   * Map<String,Object> props = new HashMap<String,Object>();
   * props.put("name", "Jill");
   * props.put("age", 33);
   * props.put("city", "Rome");
   * props.put("born", "Victoria, TX");
   * vertex.setProperties(props);
   * </code>
   *
   * @param fields
   *          Odd number of fields to set as repeating pairs of key, value, or if one parameter is received and it's a Map, the Map
   *          entries are used as field key/value pairs.
   * @param <T>
   * @return
   */
  protected <T extends OrientElement> T setPropertiesInternal(final Object... fields) {
    OrientBaseGraph graph = getGraph();
    if (fields != null && fields.length > 0 && fields[0] != null) {
      if (graph != null)
        graph.autoStartTransaction();

      if (fields.length == 1) {
        Object f = fields[0];
        if (f instanceof Map<?, ?>) {
          for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) f).entrySet())
            setPropertyInternal(this, (ODocument) rawElement.getRecord(), entry.getKey().toString(), entry.getValue());

        } else if (f instanceof Collection) {
          for (Object o : (Collection) f) {
            if (!(o instanceof OPair))
              throw new IllegalArgumentException(
                  "Invalid fields: expecting a pairs of fields as String,Object, but found the item: " + o);

            final OPair entry = (OPair) o;
            setPropertyInternal(this, (ODocument) rawElement.getRecord(), entry.getKey().toString(), entry.getValue());
          }

        } else
          throw new IllegalArgumentException(
              "Invalid fields: expecting a pairs of fields as String,Object or a single Map<String,Object>, but found: " + f);
      } else {
        if (fields.length % 2 != 0)
          throw new IllegalArgumentException(
              "Invalid fields: expecting a pairs of fields as String,Object or a single Map<String,Object>, but found: "
                  + Arrays.toString(fields));

        // SET THE FIELDS
        for (int i = 0; i < fields.length; i += 2)
          setPropertyInternal(this, (ODocument) rawElement.getRecord(), fields[i].toString(), fields[i + 1]);
      }
    }
    return (T) this;
  }
}
