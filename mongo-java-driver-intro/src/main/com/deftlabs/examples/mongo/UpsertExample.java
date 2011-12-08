/**
 * Copyright 2011, Deft Labs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.deftlabs.examples.mongo;

// Mongo
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import org.bson.types.ObjectId;

// JUnit
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

// Java
import java.util.List;
import java.util.ArrayList;

/**
 * Examples of how to upsert docs in mongo. This assumes you are running mongo on 127.0.0.1:27017
 */
public final class UpsertExample {

    /**
     * A simple upsert. When using upsert, the query is pulled into the insert.
     */
    @Test
    public void upsertSimple() throws Exception {

        final ObjectId docId = ObjectId.get();
        final ObjectId testId = ObjectId.get();

        final BasicDBObject query = new BasicDBObject("_id", docId);

        // Create the values to set.
        DBObject toSet = BasicDBObjectBuilder.start().add("testId", testId).add("foo", "bar").get();
        _mongo.getDB("mongo-java-driver-intro").getCollection("upsertExamples")
        .update(query, new BasicDBObject("$set", toSet), true, false);

        // Confirm the document was inserted
        DBObject foundDoc 
        = _mongo.getDB("mongo-java-driver-intro").getCollection("upsertExamples").findOne(new BasicDBObject("_id", docId));
        assertNotNull(foundDoc);

        // Confirm the values
        assertEquals("bar", foundDoc.get("foo"));
        assertEquals(docId, foundDoc.get("_id"));
        assertEquals(testId, foundDoc.get("testId"));

        // Run another upsert against the same doc
        final ObjectId newTestId = ObjectId.get();

        toSet = BasicDBObjectBuilder.start().add("testId", newTestId).add("foo", "notbar").get();
        _mongo.getDB("mongo-java-driver-intro").getCollection("upsertExamples")
        .update(query, new BasicDBObject("$set", toSet), true, false);

        // Confirm the document was updated
        foundDoc = _mongo.getDB("mongo-java-driver-intro").getCollection("upsertExamples").findOne(new BasicDBObject("_id", docId));

        assertEquals("notbar", foundDoc.get("foo"));
        assertEquals(docId, foundDoc.get("_id"));
        assertEquals(newTestId, foundDoc.get("testId"));
    }

    @BeforeClass
    public static void start() throws Exception {
        stop();
        _mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017/?maxpoolsize=1&waitqueuemultiple=5&connecttimeoutms=10000&sockettimeoutms=10000&autoconnectretry=true"));

        _mongo.getDB("mongo-java-driver-intro").dropDatabase();
    }

    @AfterClass
    public static void stop() {
        if (_mongo != null) _mongo.close();
    }

    private static Mongo _mongo;

}

