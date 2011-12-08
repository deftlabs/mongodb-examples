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
 * Examples of how to update docs in mongo. This assumes you are running mongo on 127.0.0.1:27017
 */
public final class UpdateExample {

    /**
     * A simple update using save.
     */
    @Test
    public void save() throws Exception {

        // Insert a doc
        final ObjectId docId = ObjectId.get();
        final DBObject doc = BasicDBObjectBuilder.start().add("_id", docId).add("foo", "bar").get();
        _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").insert(doc);

        // Confirm it was inserted
        DBObject foundDoc = _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").findOne();
        assertNotNull(foundDoc);

        // Modify the document and save
        foundDoc.put("foo", "foo");

        _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").save(foundDoc);

        // Load the saved doc
        foundDoc = _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").findOne();
        assertNotNull(foundDoc);

        // Confirm it was updated
        assertEquals("foo", foundDoc.get("foo"));

    }

    /**
     * An update using $set.
     */
    @Test
    public void updateSimpleSet() throws Exception {

        // Drop the database
        _mongo.getDB("mongo-java-driver-intro").dropDatabase();

        // Insert a doc
        final ObjectId docId = ObjectId.get();
        final DBObject doc = BasicDBObjectBuilder.start().add("_id", docId).add("foo", "bar").get();
        _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").insert(doc);

        // Confirm it was inserted
        DBObject foundDoc = _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").findOne();
        assertNotNull(foundDoc);

        // Modify the document and save
        final BasicDBObject updateQuery = new BasicDBObject("_id", docId);

        final BasicDBObject toSet = new BasicDBObject("foo", "foo");
        final BasicDBObject update = new BasicDBObject("$set", toSet);

        _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").update(updateQuery, update, false, false);

        // Load the saved doc
        foundDoc = _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").findOne();
        assertNotNull(foundDoc);

        // Confirm it was updated
        assertEquals("foo", foundDoc.get("foo"));
    }

    /**
     * An update using $inc.
     */
    @Test
    public void updateSimpleInc() throws Exception {

        // Drop the database
        _mongo.getDB("mongo-java-driver-intro").dropDatabase();

        // Insert a doc
        final ObjectId docId = ObjectId.get();
        final DBObject doc = BasicDBObjectBuilder.start().add("_id", docId).add("foo", 1).get();
        _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").insert(doc);

        // Confirm it was inserted
        DBObject foundDoc = _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").findOne();
        assertNotNull(foundDoc);

        // Modify the document and save
        final BasicDBObject updateQuery = new BasicDBObject("_id", docId);

        final BasicDBObject toIncrement = new BasicDBObject("foo", 1);
        final BasicDBObject update = new BasicDBObject("$inc", toIncrement);

        _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").update(updateQuery, update, false, false);

        // Load the saved doc
        foundDoc = _mongo.getDB("mongo-java-driver-intro").getCollection("updateExamples").findOne();
        assertNotNull(foundDoc);

        // Confirm it was updated
        assertEquals(2, foundDoc.get("foo"));
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

