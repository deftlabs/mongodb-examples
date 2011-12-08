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
import com.mongodb.MongoURI;;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

// JUnit
import org.junit.Test;
import static org.junit.Assert.*;

// Java
import java.util.List;
import java.util.ArrayList;

/**
 * Examples of how to open a connection to MongoDB.
 */
public final class ConnectionExample {

    /**
     * An example of connecting to a single mongo.
     */
    @Test
    public void standaloneServerAddress() throws Exception {
        final Mongo mongo = new Mongo(new ServerAddress("127.0.0.1", 27017));
        assertNotNull(mongo);
    }

    /**
     * An example of connecting to a single mongo with connection options.
     */
    @Test
    public void standaloneServerAddressWithOptions() throws Exception {

        final MongoOptions options = new MongoOptions();

        options.connectionsPerHost = 100;

        // Default is 10, number of thread multiplier to queue when waiting for a pooled object
        options.threadsAllowedToBlockForConnectionMultiplier = 10;

        options.maxWaitTime = 30000; // time in ms
        options.connectTimeout = 20000; // time in ms
        options.socketTimeout = 20000; // time in ms
        options.socketKeepAlive = true;
        options.autoConnectRetry = true;
        options.maxAutoConnectRetryTime = 15000; // default is 15 seconds

        // Deprecated: options.slaveOk = false;

        options.safe = false; // true == WriteConcern.SAFE for all operations
        options.w = 0; // w value of the global WriteConcern - blocks until written to N servers
        options.wtimeout = 0; // default is zero (wait forever)

        // set to true to fsync each operation - with journaling enabled, blocks until next journal commit
        options.fsync = false;

        // set to true to block on getLastError until everything has been committed
        options.j = false;

        // These are primarily for tool developers
        // options.dbDecoderFactory =
        // options.dbEncoderFactory =
        // options.socketFactory =

        final Mongo mongo = new Mongo(new ServerAddress("127.0.0.1", 27017), options);
        assertNotNull(mongo);
    }

    /**
     * An example of connecting to a replica set.
     */
    @Test
    public void replicaSetServerAddress() throws Exception {
        final List<ServerAddress> servers = new ArrayList<ServerAddress>();
        servers.add(new ServerAddress("127.0.0.1", 27017));
        servers.add(new ServerAddress("127.0.0.1", 27018));
        servers.add(new ServerAddress("127.0.0.1", 27019));
        final Mongo mongo = new Mongo(servers);
        assertNotNull(mongo);
    }

    /**
     * An example using a MongoURI
     * mongodb://127.0.0.1:27017/?maxpoolsize=10&waitqueuemultiple=5&connecttimeoutms=20000&sockettimeoutms=20000&autoconnectretry=true
     * <ul>
     * <li>maxpoolsize</li>
     * <li>waitqueuemultiple</li>
     * <li>waitqueuetimeoutms</li>
     * <li>connecttimeoutms</li>
     * <li>sockettimeoutms</li>
     * <li>autoconnectretry</li>
     * <li>slaveok</li>
     * <li>safe</li>
     * <li>w</li>
     * <li>wtimeout</li>
     * <li>fsync</li>
     * </ul>
     */
    @Test
    public void standaloneMongoUri() throws Exception {
        final Mongo mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017"));
        assertNotNull(mongo);
    }

    /**
     * A MongoURI connecting to a replica set.
     */
    @Test
    public void replicaSetMongoUri() throws Exception {
        final Mongo mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019"));
        assertNotNull(mongo);
    }
}

