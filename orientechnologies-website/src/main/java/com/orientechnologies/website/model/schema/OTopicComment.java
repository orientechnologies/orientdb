package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import java.util.Date;

/**
 * Created by Enrico Risa on 04/11/14.
 */
// Vertices
public enum OTopicComment implements OTypeHolder<Comment> {
    UUID("uuid") {
        @Override
        public OType getType() {
            return OType.STRING;
        }
    },
    COMMENT_ID("comment_id") {
        @Override
        public OType getType() {
            return OType.INTEGER;
        }
    },
    BODY("body") {
        @Override
        public OType getType() {
            return OType.STRING;
        }
    },
    USER("user") {
        @Override
        public OType getType() {
            return OType.LINK;
        }
    },

    UPDATED_AT("updatedAt") {
        @Override
        public OType getType() {
            return OType.DATETIME;
        }
    };
    private final String name;

    OTopicComment(String name) {
        this.name = name;
    }

    @Override
    public ODocument toDoc(Comment entity, OrientBaseGraph graph) {
        ODocument doc;
        if (entity.getId() == null) {
            doc = new ODocument(entity.getClass().getSimpleName());
            doc.field(UUID.toString(), java.util.UUID.randomUUID());
        } else {
            doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
        }
        doc.field(COMMENT_ID.toString(), entity.getCommentId());
        doc.field(BODY.toString(), entity.getBody());
        doc.field(OEvent.CREATED_AT.toString(), entity.getCreatedAt());
        doc.field(UPDATED_AT.toString(), entity.getUpdatedAt());
        doc.field(USER.toString(), (entity.getUser() != null ? new ORecordId(entity.getUser().getRid()) : null));
        return doc;
    }

    @Override
    public Comment fromDoc(ODocument doc, OrientBaseGraph graph) {

        Comment comment = new Comment();
        comment.setId(doc.getIdentity().toString());
        comment.setUuid((String) doc.field(UUID.toString()));
        comment.setCommentId((Integer) doc.field(COMMENT_ID.toString()));
        comment.setBody((String) doc.field(BODY.toString()));
        comment.setCreatedAt((Date) doc.field(OEvent.CREATED_AT.toString()));
        comment.setCreatedAt((Date) doc.field(OEvent.CREATED_AT.toString()));
        comment.setUser(OUser.NAME.fromDoc((ODocument) doc.field(USER.toString()), graph));
        comment.setInternal(doc);
        return comment;
    }

    @Override
    public String toString() {
        return name;
    }
}