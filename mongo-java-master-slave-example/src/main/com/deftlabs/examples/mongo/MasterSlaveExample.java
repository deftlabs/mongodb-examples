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
import com.mongodb.MongoURI;
import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;

// JUnit
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

// Java
import java.util.List;
import java.util.ArrayList;

/**
 * An example of how to setup/use a master/slave configuration.
 */
public final class MasterSlaveExample {

    private static final int ITERATIONS = 100;

    @Before
    public void setupMongo() throws Exception {
        // The Java driver does not load balance requests to the master/slave.
        _mongoMaster = new Mongo(new MongoURI("mongodb://127.0.0.1:27017"));
        _mongoSlave = new Mongo(new MongoURI("mongodb://127.0.0.1:27018"));
        _mongoSlave.slaveOk();
    }

    @Test
    public void checkWritesReads() throws Exception {

        // Write some data
        for (int idx=0; idx < ITERATIONS; idx++) {
            final BasicDBObject obj = new BasicDBObject();
            obj.put("idx", idx);
            _mongoMaster.getDB("test").getCollection("testMasterSlave").insert(obj);
        }

        Thread.sleep(2000);

        for (int idx=0; idx < ITERATIONS; idx++) {
            final BasicDBObject obj
            = (BasicDBObject)_mongoSlave.getDB("test").getCollection("testMasterSlave").findOne(new BasicDBObject("idx", idx));

            assertNotNull(obj);
        }
    }

    private Mongo _mongoMaster;
    private Mongo _mongoSlave;
}

