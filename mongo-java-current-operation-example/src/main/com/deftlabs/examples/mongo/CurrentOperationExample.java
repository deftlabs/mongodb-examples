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

// JUnit
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Get the current db operation in Java.
 */
public final class CurrentOperationExample {

    private Mongo _mongo;

    @Test
    @SuppressWarnings("unchecked")
    public void currentOperation() throws Exception {
        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "admin"));

        // Current operation.
        final BasicDBObject one
        = (BasicDBObject)mongo.getDB("admin").getCollection("$cmd.sys.inprog").findOne();

        System.out.println(one);

        // For all connections.
        DBCursor cur
        = mongo.getDB("admin").getCollection("$cmd.sys.inprog").find(new BasicDBObject("$all", 1));

        while (cur.hasNext()) System.out.println((BasicDBObject)cur.next());
    }
}

