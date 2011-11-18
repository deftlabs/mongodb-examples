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
import com.mongodb.Mongo;
import com.mongodb.DBAddress;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

// JUnit
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test Double.NaN etc.
 */
public final class DoubleExample {

    private Mongo _mongo;

    @Test
    public void testSimple() throws Exception {

        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "test"));

        mongo.getDB("test").getCollection("testDouble").drop();

        final BasicDBObject testDouble = new BasicDBObject();
        testDouble.put("MAX_VALUE", Double.MAX_VALUE);
        testDouble.put("MIN_VALUE", Double.MIN_VALUE);
        testDouble.put("NaN", Double.NaN);
        testDouble.put("NEGATIVE_INFINITY", Double.NEGATIVE_INFINITY);
        testDouble.put("POSITIVE_INFINITY", Double.POSITIVE_INFINITY );

        mongo.getDB("test").getCollection("testDouble").update(new BasicDBObject("_id", ObjectId.get()), new BasicDBObject("$set", testDouble), true, false);

        final BasicDBObject from = (BasicDBObject)mongo.getDB("test").getCollection("testDouble").findOne();

        assertNotNull(from);

        assertEquals(Double.MAX_VALUE, from.getDouble("MAX_VALUE"), (double)0);
        assertEquals(Double.MIN_VALUE, from.getDouble("MIN_VALUE"), (double)0);
        assertEquals(Double.NaN, from.getDouble("NaN"), (double)0);
        assertEquals(Double.NEGATIVE_INFINITY, from.getDouble("NEGATIVE_INFINITY"), (double)0);
        assertEquals(Double.POSITIVE_INFINITY, from.getDouble("POSITIVE_INFINITY"), (double)0);
    }

    @Test
    public void testSimpleBson() throws Exception {

        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "test"));

        mongo.getDB("test").getCollection("testDouble").drop();

        final BasicBSONObject test = new BasicBSONObject();
        test.put("MAX_VALUE", Double.MAX_VALUE);
        test.put("MIN_VALUE", Double.MIN_VALUE);
        test.put("NaN", Double.NaN);
        test.put("NEGATIVE_INFINITY", Double.NEGATIVE_INFINITY);
        test.put("POSITIVE_INFINITY", Double.POSITIVE_INFINITY );

        //mongo.getDB("test").getCollection("testDouble").insert(new BasicDBObject("nested", test));

        mongo.getDB("test").getCollection("testDouble").update(new BasicDBObject("_id", ObjectId.get()), new BasicDBObject("$set", new BasicDBObject("nested", test)), true, false);

        final BasicDBObject from = (BasicDBObject)mongo.getDB("test").getCollection("testDouble").findOne();

        assertNotNull(from);

        final BasicBSONObject fromBson = (BasicBSONObject)from.get("nested");

        assertEquals(Double.MAX_VALUE, fromBson.getDouble("MAX_VALUE"), (double)0);
        assertEquals(Double.MIN_VALUE, fromBson.getDouble("MIN_VALUE"), (double)0);
        assertEquals(Double.NaN, fromBson.getDouble("NaN"), (double)0);
        assertEquals(Double.NEGATIVE_INFINITY, fromBson.getDouble("NEGATIVE_INFINITY"), (double)0);
        assertEquals(Double.POSITIVE_INFINITY, fromBson.getDouble("POSITIVE_INFINITY"), (double)0);
    }
}

