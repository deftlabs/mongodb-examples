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
 * Examples of how to insert into mongo. This assumes you are running mongo on 127.0.0.1:27017
 */
public final class InsertExample {

    /**
     * A simple insert.
     */
    @Test
    public void insert() throws Exception {

        // Get the object id
        final ObjectId newDocId = ObjectId.get();

        // Insert the document
        final DBObject doc = BasicDBObjectBuilder.start().add("_id", newDocId)
        .add("testId", ObjectId.get())
        .add("foo", "bar").get();

        _mongo.getDB("mongo-java-driver-intro").getCollection("insertExamples").insert(doc);

        // Lookup the same document
        final DBObject foundDoc
        = _mongo.getDB("mongo-java-driver-intro").getCollection("insertExamples").
        findOne(new BasicDBObject("_id", newDocId));

        assertNotNull(foundDoc);
    }

    /**
     * A simple save. Save can be used for insert and update.
     */
    @Test
    public void save() throws Exception {

        // Get the object id
        final ObjectId newDocId = ObjectId.get();

        // Insert the document
        final DBObject doc = BasicDBObjectBuilder.start().add("_id", newDocId).add("testId", ObjectId.get()).add("foo", "bar").get();
        _mongo.getDB("mongo-java-driver-intro").getCollection("saveExamples").save(doc);

        // Lookup the same document
        final DBObject foundDoc = _mongo.getDB("mongo-java-driver-intro").getCollection("saveExamples").findOne(new BasicDBObject("_id", newDocId));
        assertNotNull(foundDoc);
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

