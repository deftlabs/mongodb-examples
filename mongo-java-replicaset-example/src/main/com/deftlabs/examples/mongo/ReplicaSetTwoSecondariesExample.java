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
import java.util.ArrayList;

/**
 * An example of how to setup/use a replica set.
 */
public final class ReplicaSetTwoSecondariesExample {

    private static final int [] _ports = { 27017, 27018, 27019 };

    @Before
    public void setupMongo() throws Exception {

        final BasicDBObject config = new BasicDBObject("_id", "test");

        final List<BasicDBObject> servers = new ArrayList<BasicDBObject>();

        int idx=0;
        for (final int port : _ports) {
            final BasicDBObject server = new BasicDBObject("_id", idx);
            server.put("host", ("127.0.0.1:" + port));

            // We're going to specify that only the first server can be the master.
            // Additionally, we're going to configure the slaves to be 5 seconds behind
            // the master.
            if (idx > 0 && idx != 3) {
                //server.put("priority", 0);
                //server.put("slaveDelay", 1);
            }

            //if (idx == 2) server.put("arbiterOnly", true);

            servers.add(server);

            idx++;
        }

        config.put("members", servers);

        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "admin"));

        final CommandResult result
        = mongo.getDB("admin").command(new BasicDBObject("replSetInitiate", config));

        System.out.println(result);

        // Sleep for a bit to wait for all the nodes to be intialized.
        Thread.sleep(5000);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void verifySetMembers() throws Exception {

        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "admin"));

        final CommandResult result
        = mongo.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));

        final List<BasicDBObject> members = (List<BasicDBObject>)result.get("members");

        assertEquals(3, members.size());

        for (final BasicDBObject member : members) {
            //System.out.println(member);
        }
    }
}

