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
 * Examples of how to query in mongo. This assumes you are running mongo on 127.0.0.1:27017
 */
public final class QueryExample {

    /**
     * A simple find one query.
     */
    @Test
    public void simpleFindOne() throws Exception {
        final BasicDBObject doc = 
        (BasicDBObject)_mongo.getDB("mongo-java-driver-intro").getCollection("queryExamples")
        .findOne(new BasicDBObject("_id", 1));
        assertNotNull(doc);
    }

    /**
     * A find one query that limits the fields returned.
     */
    @Test
    public void findOneWithFields() throws Exception {
        final BasicDBObject query = new BasicDBObject("_id", 1);
        final BasicDBObject fields = new BasicDBObject("testId", 1);

        final DBObject doc
        = _mongo.getDB("mongo-java-driver-intro").getCollection("queryExamples").findOne(query, fields);
        assertNotNull(doc);

        assertNotNull(doc.get("testId"));
        assertNull(doc.get("foo"));
    }

    /**
     * A query that finds multiple docs and converts to an array.
     */
    @Test
    public void findMultipleWithToArray() throws Exception {
        DBCursor cur = null;
        try {
            cur = _mongo.getDB("mongo-java-driver-intro").getCollection("queryExamples").find();

            assertNotNull(cur);

            final List<DBObject> docs = cur.toArray();

            assertEquals(10, docs.size());

        } finally { cur.close(); }
    }

    /**
     * A query that finds multiple documents and iterates over cursor.
     */
    @Test
    public void findMultipleCurWalk() throws Exception {
        DBCursor cur = null;
        try {
            cur = _mongo.getDB("mongo-java-driver-intro").getCollection("queryExamples")
            .find().sort(new BasicDBObject("_id", 1)).batchSize(10);

            assertNotNull(cur);

            int idx = 0;

            while (cur.hasNext()) {
                final BasicDBObject doc = (BasicDBObject)cur.next();
                assertEquals(idx, doc.getInt("_id"));
                idx++;
            }

        } finally { cur.close(); }
    }

    @BeforeClass
    public static void start() throws Exception {
        stop();
        _mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017/?maxpoolsize=1&waitqueuemultiple=5&connecttimeoutms=10000&sockettimeoutms=10000&autoconnectretry=true"));

        _mongo.getDB("mongo-java-driver-intro").dropDatabase();

        // Insert some test data.
        for (int idx=0; idx < 10; idx++) {
            final DBObject doc = BasicDBObjectBuilder.start().add("_id", idx).add("testId", ObjectId.get()).add("foo", "bar").get();
            _mongo.getDB("mongo-java-driver-intro").getCollection("queryExamples").insert(doc);
        }
    }

    @AfterClass
    public static void stop() {
        if (_mongo != null) _mongo.close();

    }

    private static Mongo _mongo;

}

