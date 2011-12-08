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
import com.mongodb.*;
import org.bson.types.*;

// JUnit
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

/**
 * Examples of using write concerns in mongo. This assumes you are running mongo on 127.0.0.1:27017
 */
public final class WriteConcernExample {

    /**
     * A simple insert write concern.
     */
    @Test
    public void writeConcern() throws Exception {

        // Insert a test object.
        final ObjectId docId = ObjectId.get();
        _mongo.getDB("mongo-java-driver-intro").getCollection("writeConcern").insert(new BasicDBObject("_id", docId), WriteConcern.SAFE);


        Throwable mongoException = null;
        try {
            // This should fail because the _id is duplicated
            final WriteResult result
            = _mongo.getDB("mongo-java-driver-intro").getCollection("writeConcern").insert(new BasicDBObject("_id", docId), WriteConcern.SAFE);
        } catch (final MongoException me) { mongoException = me; }

        assertNotNull(mongoException);

        // This is an example of NORMAL exception handling - this insert fails because the _id
        // is a duplicate
        final WriteResult result
        = _mongo.getDB("mongo-java-driver-intro").getCollection("writeConcern").insert(new BasicDBObject("_id", docId), WriteConcern.NORMAL);

        // You now have the ability to look at the exception and error codes so you can
        // decide how to handle the exception
        assertEquals(true, result.getLastError().getException() instanceof MongoException.DuplicateKey);
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

