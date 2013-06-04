/**
 * Copyright 2013, Deft Labs.
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

package com.deftlabs.tests.mongo;

// Mongo
import com.mongodb.*;
import org.bson.types.*;

// JUnit
import org.junit.*;
import static org.junit.Assert.*;

// java
import java.util.*;

/**
 * A test to see the performance impact of using $setOnInsert.
 */
public final class SetOnInsertTest {

    private static final int TEST_UPDATES = 1000;

    private static final int TEST_ITERATIONS = 100;

    private MongoClient _mongoClient;


    @Test public void testWithSetOnInsert() throws Exception {
        clearCollection();
        final long startTime = System.currentTimeMillis();

        final Set<ObjectId> docIds = runTest(true);

        Thread.sleep(1000);

        verifyData(docIds);

        final long execTime = System.currentTimeMillis() - startTime;
        System.out.println("----- testWithSetOnInsert: " + execTime + " (ms)");
    }


    @Test public void testWithoutSetOnInsert() throws Exception {
        clearCollection();
        final long startTime = System.currentTimeMillis();

        final Set<ObjectId> docIds = runTest(false);

        Thread.sleep(1000);

        verifyData(docIds);

        final long execTime = System.currentTimeMillis() - startTime;
        System.out.println("----- testWithoutSetOnInsert: " + execTime + " (ms)");
    }

    private void verifyData(final Set<ObjectId> pDocIds) {

        for (final ObjectId docId : pDocIds) {

            final BasicDBObject doc = (BasicDBObject)getCollection().findOne(new BasicDBObject("_id", docId));

            if (doc == null) { System.out.println("----- what? a null doc"); continue; }
            if (doc.get("groupId") == null) System.out.println("----- what? a missing group id");
            if (doc.get("hostId") == null) System.out.println("----- what? a missing host id");
            if (doc.get("group") == null) System.out.println("----- what? a missing group");
            if (doc.get("identity") == null) System.out.println("----- what? a missing identity");
            if (doc.get("date") == null) System.out.println("----- what? a missing date");

            final Integer counter = doc.getInt("counter");
            if (counter == null) { System.out.println("----- what? counter is null"); continue; }

            if (counter != TEST_UPDATES) {
                System.out.println("----- what? expecting the counter to be: " + TEST_UPDATES + " - it was: " + counter);
            }
        }
    }

    private Set<ObjectId> runTest(final boolean pSetOnInsert) {

        final Set<ObjectId> docIds = new HashSet<ObjectId>();

        for (int idx0=0; idx0 < TEST_ITERATIONS; idx0++) {

            final ObjectId docId = ObjectId.get();
            final ObjectId groupId = ObjectId.get();
            final ObjectId hostId = ObjectId.get();
            final String group = "testGroup";
            final String identity = "testIdentity";
            final String date = "20130604";

            docIds.add(docId);

            final DBObject toSetOnInsert
            = BasicDBObjectBuilder.start().add("groupId", groupId).add("hostId", hostId).add("group", group).add("identity", identity).add("date", date).get();

            final DBObject toSet
            = BasicDBObjectBuilder.start().add("groupId", groupId).add("hostId", hostId).add("group", group).add("identity", identity).add("date", date).get();

            // This is just a test to confirm it is setting on insert.
            if (pSetOnInsert) {
                getCollection().update(new BasicDBObject("_id", docId), new BasicDBObject("$setOnInsert", toSetOnInsert), true, false);
            } else {
                getCollection().update(new BasicDBObject("_id", docId), new BasicDBObject("$set", toSet), true, false);
            }

            for (int idx1=0; idx1 < TEST_UPDATES ; idx1++) {
                if (pSetOnInsert) {

                    final BasicDBObject ops = new BasicDBObject("$inc", new BasicDBObject("counter", 1));
                    ops.put("$setOnInsert", toSetOnInsert);

                    getCollection().update(new BasicDBObject("_id", docId), ops, true, false);

                } else {

                    final BasicDBObject ops = new BasicDBObject("$inc", new BasicDBObject("counter", 1));
                    ops.put("$set", toSet);

                    getCollection().update(new BasicDBObject("_id", docId), ops, true, false);
                }
            }
        }

        return docIds;
    }

    private DBCollection getCollection() { return getCollection("setOnInsert"); }

    private void clearCollection() {
        getCollection().remove(new BasicDBObject());
        getCollection().createIndex(BasicDBObjectBuilder.start().add("groupId", 1).add("hostId", 1).add("group", 1).add("identity", 1).add("date", 1).get());
    }

    @Before public void before() throws Exception { _mongoClient = new MongoClient(new DBAddress("127.0.0.1", 27017, "test")); }

    @After public void after() throws Exception { if (_mongoClient != null) _mongoClient.close(); }

    private DBCollection getCollection(final String pName) { return _mongoClient.getDB("test").getCollection(pName); }
}

