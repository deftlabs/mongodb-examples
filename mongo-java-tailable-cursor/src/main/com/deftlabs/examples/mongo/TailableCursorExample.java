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
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

// Java
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashSet;
import java.util.ArrayList;

/**
 * Show how to use a tailable cursor.
 */
public final class TailableCursorExample {

    public static void main(final String [] pArgs) throws Exception {
        final Mongo mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017"));

        mongo.getDB("testTailableCursor").dropDatabase();

        // Create the capped collection
        final BasicDBObject conf = new BasicDBObject("capped", true);
        conf.put("size",  20971520); // 20 MB
        mongo.getDB("testTailableCursor").createCollection("test", conf);

        final AtomicBoolean readRunning = new AtomicBoolean(true);
        final AtomicBoolean writeRunning = new AtomicBoolean(true);

        final AtomicLong writeCounter = new AtomicLong(0);
        final AtomicLong readCounter = new AtomicLong(0);

        final ArrayList<Thread> writeThreads = new ArrayList<Thread>();
        final ArrayList<Thread> readThreads = new ArrayList<Thread>();

        for (int idx=0; idx < 10; idx++) {
            final Thread writeThread = new Thread(new Writer(mongo, writeRunning, writeCounter));
            final Thread readThread = new Thread(new Reader(mongo, readRunning, readCounter));
            writeThread.start();
            readThread.start();
            writeThreads.add(writeThread);
            readThreads.add(readThread);
        }

        // Run for five minutes
        //Thread.sleep(300000);
        Thread.sleep(20000);
        writeRunning.set(false);
        Thread.sleep(5000);
        readRunning.set(false);
        Thread.sleep(5000);

        for (final Thread readThread : readThreads) readThread.interrupt();
        for (final Thread writeThread : writeThreads) writeThread.interrupt();

        System.out.println("----- write count: " + writeCounter.get());
        System.out.println("----- read count: " + readCounter.get());
    }

    /**
     * The thread that is reading from the capped collection.
     */
    private static class Reader implements Runnable {
        @Override
        public void run() {
            final HashSet<ObjectId> seenIds = new HashSet<ObjectId>();
            long lastTimestamp = 0;

            while (_running.get()) {
                try {
                    _mongo.getDB("testTailableCursor").requestStart();
                    final DBCursor cur = createCursor(lastTimestamp);
                    try {
                        while (cur.hasNext() && _running.get()) {
                            final BasicDBObject doc = (BasicDBObject)cur.next();
                            final ObjectId docId = doc.getObjectId("_id");
                            lastTimestamp = doc.getLong("ts");
                            if (seenIds.contains(docId)) System.out.println("------ duplicate id found: " + docId);
                            seenIds.add(docId);
                            _counter.incrementAndGet();
                        }
                    } finally {
                        try { if (cur != null) cur.close(); } catch (final Throwable t) { /* nada */ }
                        _mongo.getDB("testTailableCursor").requestDone();
                    }

                    try { Thread.sleep(100); } catch (final InterruptedException ie) { break; }
                } catch (final Throwable t) { t.printStackTrace(); }
            }
        }

        private DBCursor createCursor(final long pLast) {
            final DBCollection col = _mongo.getDB("testTailableCursor").getCollection("test");

            if (pLast == 0)
            { return col.find().sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE).addOption(Bytes.QUERYOPTION_AWAITDATA); }

            final BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", pLast));
            return col.find(query).sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE).addOption(Bytes.QUERYOPTION_AWAITDATA);
        }

        private Reader(final Mongo pMongo, final AtomicBoolean pRunning, final AtomicLong pCounter)
        { _mongo = pMongo; _running = pRunning; _counter = pCounter; }

        private final Mongo _mongo;
        private final AtomicBoolean _running;
        private final AtomicLong _counter;
    }

    /**
     * The thread that is writing to the capped collection.
     */
    private static class Writer implements Runnable {
        @Override
        public void run() {
            while (_running.get()) {
                final ObjectId docId = ObjectId.get();
                final BasicDBObject doc = new BasicDBObject("_id", docId);
                final long count = _counter.incrementAndGet();
                doc.put("count", count);
                doc.put("ts", System.currentTimeMillis());
                _mongo.getDB("testTailableCursor").getCollection("test").insert(doc);
            }
        }

        private Writer(final Mongo pMongo, final AtomicBoolean pRunning, final AtomicLong pCounter)
        { _mongo = pMongo; _running = pRunning; _counter = pCounter; }

        private final Mongo _mongo;
        private final AtomicBoolean _running;
        private final AtomicLong _counter;
    }
}

