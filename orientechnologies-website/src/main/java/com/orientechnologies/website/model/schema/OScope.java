package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.helper.OSequenceHelper;
import com.orientechnologies.website.model.schema.dto.Scope;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 24/11/14.
 */
public enum OScope implements OTypeHolder<Scope> {

    NAME("name") {
        @Override
        public OType getType() {
            return OType.STRING;
        }
    },
    NUMBER("number") {
        @Override
        public OType getType() {
            return OType.INTEGER;
        }
    };

    private String name;

    OScope(String name) {

        this.name = name;
    }

    @Override
    public Scope fromDoc(ODocument doc, OrientBaseGraph graph) {

        if (doc == null) {
            return null;
        }
        Scope l = new Scope();
        l.setId(doc.getIdentity().toString());
        l.setName((String) doc.field(NAME.toString()));
        l.setNumber((Integer) doc.field(NUMBER.toString()));
        OrientVertex iss = graph.getVertex(doc);
        for (Vertex vertex : iss.getVertices(Direction.IN, HasScope.class.getSimpleName())) {
            l.setRepository(ORepository.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
            break;
        }
        for (Vertex vertex : iss.getVertices(Direction.OUT, HasOwner.class.getSimpleName())) {
            l.setOwner(OUser.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
            break;
        }
        List<com.orientechnologies.website.model.schema.dto.OUser> members = new ArrayList<com.orientechnologies.website.model.schema.dto.OUser>();
        for (Vertex vertex : iss.getVertices(Direction.OUT, HasMember.class.getSimpleName())) {
            members.add(OUser.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
        }
        l.setMembers(members);
        return l;
    }

    @Override
    public ODocument toDoc(Scope entity, OrientBaseGraph graph) {

        ODocument doc;
        if (entity.getId() == null) {
            doc = new ODocument(entity.getClass().getSimpleName());
            Long id = OSequenceHelper.next(graph, entity.getClass().getSimpleName());
            doc.field(NUMBER.toString(), id.intValue());
        } else {
            doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
            doc.field(NUMBER.toString(), entity.getNumber());
        }
        doc.field(NAME.toString(), entity.getName());
        return doc;
    }

    @Override
    public String toString() {
        return name;
    }
}
