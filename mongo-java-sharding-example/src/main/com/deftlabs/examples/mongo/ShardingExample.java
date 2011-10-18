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
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DBAddress;
import com.mongodb.ServerAddress;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;

// JUnit
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

// Java
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.security.MessageDigest;

/**
 * An example of how to setup/use sharding.
 */
public final class ShardingExample {

    private static final int [] _shardPorts = { 27018, 27019 };

    @Before
    public void setupCluster() throws Exception {

        // Connect to mongos
        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "admin"));

        // Add the shards
        for (final int shardPort : _shardPorts) {
            final CommandResult result
            = mongo.getDB("admin").command(new BasicDBObject("addshard", ("localhost:" + shardPort)));
            System.out.println(result);
        }

        // Sleep for a bit to wait for all the nodes to be intialized.
        Thread.sleep(5000);

        // Enable sharding on a collection.
        CommandResult result
        = mongo.getDB("admin").command(new BasicDBObject("enablesharding", "test"));
        System.out.println(result);

        final BasicDBObject shardKey = new BasicDBObject("date", 1);
        shardKey.put("hash", 1);

        final BasicDBObject cmd = new BasicDBObject("shardcollection", "test.logs");

        cmd.put("key", shardKey);

        result = mongo.getDB("admin").command(cmd);

        System.out.println(result);
    }

    @Test
    public void testShards() throws Exception {

        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "test"));

        final DBCollection shardCollection = mongo.getDB("test").getCollection("logs");

        final Random random = new Random(System.currentTimeMillis());

        // Write some data
        for (int idx=0; idx < 1000; idx++) {

            final BasicDBObject entry
            = new BasicDBObject("date", ("201101" + String.format("%02d", random.nextInt(30))));

            entry.put("hash", md5(("this is a value to hash-" + idx)));

            shardCollection.insert(entry);
        }
    }

    private byte [] md5(final String pValue) throws Exception
    { return MessageDigest.getInstance("MD5").digest(pValue.getBytes("UTF-8")); }
}

