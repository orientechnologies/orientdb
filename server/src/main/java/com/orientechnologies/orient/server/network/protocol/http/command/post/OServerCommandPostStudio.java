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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("unchecked")
 public class OServerCommandPostStudio extends OServerCommandAuthenticatedDbAbstract {
   private static final String[] NAMES = { "POST|studio/*" };

  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
     ODatabaseDocument db = null;

     try {
       final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: studio/<database>/<context>");

       db = getProfiledDatabaseInstance(iRequest);

       final String req = iRequest.content;

       // PARSE PARAMETERS
       String operation = null;
       String rid = null;
       String className = null;

       final Map<String, String> fields = new HashMap<String, String>();

       final String[] params = req.split("&");
       String value;

       for (String p : params) {
         String[] pairs = p.split("=");
         value = pairs.length == 1 ? null : pairs[1];

         if ("oper".equals(pairs[0]))
           operation = value;
         else if ("0".equals(pairs[0]))
           rid = value;
         else if ("1".equals(pairs[0]))
           className = value;
         else if (pairs[0].startsWith(ODocumentHelper.ATTRIBUTE_CLASS))
           className = value;
         else if (pairs[0].startsWith("@") || pairs[0].equals("id"))
           continue;
         else {
           fields.put(pairs[0], value);
         }
       }

       String context = urlParts[2];
       if ("document".equals(context))
         executeDocument(iRequest, iResponse, db, operation, rid, className, fields);
       else if ("classes".equals(context))
         executeClasses(iRequest, iResponse, db, operation, rid, className, fields);
       else if ("clusters".equals(context))
         executeClusters(iRequest, iResponse, db, operation, rid, className, fields);
       else if ("classProperties".equals(context))
         executeClassProperties(iRequest, iResponse, db, operation, rid, className, fields);
       else if ("classIndexes".equals(context))
         executeClassIndexes(iRequest, iResponse, db, operation, rid, className, fields);

     } finally {
       if (db != null)
         db.close();
     }
     return false;
   }

   private void executeClassProperties(final OHttpRequest iRequest, final OHttpResponse iResponse, final ODatabaseDocument db,
       final String operation, final String rid, final String className, final Map<String, String> fields) throws IOException {
     // GET THE TARGET CLASS
     final OClass cls = db.getMetadata().getSchema().getClass(rid);
     if (cls == null) {
       iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, "Error", OHttpUtils.CONTENT_TEXT_PLAIN, "Error: Class '" + rid
           + "' not found.", null);
       return;
     }

     if ("add".equals(operation)) {
       iRequest.data.commandInfo = "Studio add property";

       try {
         OType type = OType.valueOf(fields.get("type"));

         OPropertyImpl prop;
         if (type == OType.LINK || type == OType.LINKLIST || type == OType.LINKSET || type == OType.LINKMAP)
           prop = (OPropertyImpl) cls.createProperty(fields.get("name"), type,
               db.getMetadata().getSchema().getClass(fields.get("linkedClass")));
         else
           prop = (OPropertyImpl) cls.createProperty(fields.get("name"), type);

         if (fields.get("linkedType") != null)
           prop.setLinkedType(OType.valueOf(fields.get("linkedType")));
         if (fields.get("mandatory") != null)
           prop.setMandatory("on".equals(fields.get("mandatory")));
         if (fields.get("readonly") != null)
           prop.setReadonly("on".equals(fields.get("readonly")));
         if (fields.get("notNull") != null)
           prop.setNotNull("on".equals(fields.get("notNull")));
         if (fields.get("min") != null)
           prop.setMin(fields.get("min"));
         if (fields.get("max") != null)
           prop.setMax(fields.get("max"));

         iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Property " + fields.get("name")
             + " created successfully", null);

       } catch (Exception e) {
         iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, "Error on creating a new property in class " + rid + ": " + e,
             OHttpUtils.CONTENT_TEXT_PLAIN, "Error on creating a new property in class " + rid + ": " + e, null);
       }
     } else if ("del".equals(operation)) {
       iRequest.data.commandInfo = "Studio delete property";

       cls.dropProperty(className);

       iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Property " + fields.get("name")
           + " deleted successfully.", null);
     }
   }

   private void executeClasses(final OHttpRequest iRequest, final OHttpResponse iResponse, final ODatabaseDocument db,
       final String operation, final String rid, final String className, final Map<String, String> fields) throws IOException {
     if ("add".equals(operation)) {
       iRequest.data.commandInfo = "Studio add class";

       // int defCluster = fields.get("defaultCluster") != null ? Integer.parseInt(fields.get("defaultCluster")) : db
       // .getDefaultClusterId();

       try {
         final String superClassName = fields.get("superClass");
         final OClass superClass;
         if (superClassName != null)
           superClass = db.getMetadata().getSchema().getClass(superClassName);
         else
           superClass = null;

         final OClass cls = db.getMetadata().getSchema().createClass(fields.get("name"), superClass);

         final String alias = fields.get("alias");
         if (alias != null)
           cls.setShortName(alias);

         iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Class '" + rid
             + "' created successfully with id=" + db.getMetadata().getSchema().getClasses().size(), null);

       } catch (Exception e) {
         iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, "Error on creating the new class '" + rid + "': " + e,
             OHttpUtils.CONTENT_TEXT_PLAIN, "Error on creating the new class '" + rid + "': " + e, null);
       }
     } else if ("del".equals(operation)) {
       iRequest.data.commandInfo = "Studio delete class";

       db.getMetadata().getSchema().dropClass(rid);

       iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Class '" + rid + "' deleted successfully.",
           null);
     }
   }

   private void executeClusters(final OHttpRequest iRequest, final OHttpResponse iResponse, final ODatabaseDocument db,
       final String operation, final String rid, final String iClusterName, final Map<String, String> fields) throws IOException {
     if ("add".equals(operation)) {
       iRequest.data.commandInfo = "Studio add cluster";

       int clusterId = db.addCluster(fields.get("name"));

       iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Cluster " + fields.get("name")
           + "' created successfully with id=" + clusterId, null);

     } else if ("del".equals(operation)) {
       iRequest.data.commandInfo = "Studio delete cluster";

       db.dropCluster(rid, false);

       iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Cluster " + fields.get("name")
           + "' deleted successfully", null);
     }
   }

   private void executeDocument(final OHttpRequest iRequest, final OHttpResponse iResponse, final ODatabaseDocument db,
       final String operation, final String rid, final String className, final Map<String, String> fields) throws IOException {
     if ("edit".equals(operation)) {
       iRequest.data.commandInfo = "Studio edit document";

       if (rid == null)
         throw new IllegalArgumentException("Record ID not found in request");

       ODocument doc = new ODocument(className, new ORecordId(rid));
       doc.reload(null, true);

       // BIND ALL CHANGED FIELDS
       for (Entry<String, String> f : fields.entrySet()) {
         final Object oldValue = doc.rawField(f.getKey());
         String userValue = f.getValue();

         if (userValue != null && userValue.equals("undefined"))
           doc.removeField(f.getKey());
         else {
           Object newValue = ORecordSerializerStringAbstract.getTypeValue(userValue);

           if (newValue != null) {
             if (newValue instanceof Collection) {
               final ArrayList<Object> array = new ArrayList<Object>();
               for (String s : (Collection<String>) newValue) {
                 Object v = ORecordSerializerStringAbstract.getTypeValue(s);
                 array.add(v);
               }
               newValue = array;
             }
           }

           if (oldValue != null && oldValue.equals(userValue))
             // NO CHANGES
             continue;

           doc.field(f.getKey(), newValue);
         }
       }

       doc.save();
       iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid + " updated successfully.",
           null);
     } else if ("add".equals(operation)) {
       iRequest.data.commandInfo = "Studio create document";

       final ODocument doc = new ODocument(className);

       // BIND ALL CHANGED FIELDS
       for (Entry<String, String> f : fields.entrySet())
         doc.field(f.getKey(), f.getValue());

       doc.save();
       iResponse.send(201, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + doc.getIdentity() + " updated successfully.", null);

     } else if ("del".equals(operation)) {
       iRequest.data.commandInfo = "Studio delete document";

       if (rid == null)
         throw new IllegalArgumentException("Record ID not found in request");

       final ODocument doc = new ODocument(new ORecordId(rid));
       doc.load();
       doc.delete();
       iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid + " deleted successfully.",
           null);

     } else
       iResponse.send(500, "Error", OHttpUtils.CONTENT_TEXT_PLAIN, "Operation not supported", null);
   }

   private void executeClassIndexes(final OHttpRequest iRequest, final OHttpResponse iResponse, final ODatabaseDocument db,
       final String operation, final String rid, final String className, final Map<String, String> fields) throws IOException {
     // GET THE TARGET CLASS
     final OClass cls = db.getMetadata().getSchema().getClass(rid);
     if (cls == null) {
       iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, "Error", OHttpUtils.CONTENT_TEXT_PLAIN, "Error: Class '" + rid
           + "' not found.", null);
       return;
     }

     if ("add".equals(operation)) {
       iRequest.data.commandInfo = "Studio add index";

       try {
         final String[] fieldNames = OPatternConst.PATTERN_COMMA_SEPARATED.split(fields.get("fields").trim());
         final String indexType = fields.get("type");

         cls.createIndex(fields.get("name"), indexType, fieldNames);

         iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Index " + fields.get("name")
             + " created successfully", null);

       } catch (Exception e) {
         iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, "Error on creating a new index for class " + rid + ": " + e,
             OHttpUtils.CONTENT_TEXT_PLAIN, "Error on creating a new index for class " + rid + ": " + e, null);
       }
     } else if ("del".equals(operation)) {
       iRequest.data.commandInfo = "Studio delete index";

       try {
         final OIndex<?> index = cls.getClassIndex(className);
         if (index == null) {
           iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, "Error", OHttpUtils.CONTENT_TEXT_PLAIN, "Error: Index '" + className
               + "' not found in class '" + rid + "'.", null);
           return;
         }

         db.getMetadata().getIndexManager().dropIndex(index.getName());

         iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Index " + className
             + " deleted successfully.", null);
       } catch (Exception e) {
         iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, "Error on deletion index '" + className + "' for class " + rid + ": "
             + e, OHttpUtils.CONTENT_TEXT_PLAIN, "Error on deletion index '" + className + "' for class " + rid + ": " + e, null);
       }
     } else
       iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, "Error", OHttpUtils.CONTENT_TEXT_PLAIN, "Operation not supported", null);
   }

   public String[] getNames() {
     return NAMES;
   }
 }
