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

package com.deftlabs.tests.mongo;

// Mongo
import com.mongodb.Mongo;
import com.mongodb.DBCursor;
import com.mongodb.DBCollection;
import com.mongodb.DBAddress;
import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.MongoException;
import com.mongodb.CommandResult;

// Java
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A Java test of durability.
 *
 * Tested against 1.8.0-rc0-pre-
 *
 * This test launches Mongo, so you need to make sure the binary is in your
 * path.
 *
 * Test:
 * Launch Mongo
 * Perform some write operations
 * Kill Mongo while inserting
 * Start Mongo and verify counts
 *
 */
public class DurabilityTest {

    private static final int TEST_TASKS = 1000;

    private Mongo _mongo;
    private final Timer _timer = new Timer();
    private final AtomicBoolean _mongoRunning = new AtomicBoolean(false);
    private final ReentrantLock _lock = new ReentrantLock(true);
    private volatile Process _mongoProcess;
    private TaskWriter _taskWriter;

    private final ArrayBlockingQueue<BasicDBObject> _tasks
    = new ArrayBlockingQueue<BasicDBObject>(TEST_TASKS);

    public static void main(final String [] pArgs) throws Exception {
        final DurabilityTest test = new DurabilityTest();
        test.setup();

        while (test.processingTasks()) { Thread.sleep(1000); }

        Thread.sleep(5000);

        test.finalize();
    }

    public void setup() throws Exception {
        // Load up the test tasks to process.
        for (int idx=0; idx < TEST_TASKS; idx++) {
            final BasicDBObject obj = new BasicDBObject("count", idx);
            obj.put("test", TEST_DATA);
            _tasks.put(obj);
        }

        _timer.scheduleAtFixedRate(new MongoRunnerKillerTask(), 0, 10000);

        _taskWriter = new TaskWriter();
        _taskWriter.start();
    }

    public boolean processingTasks() { return (!_tasks.isEmpty()); }

    public void finalize() throws Exception {
        try {
            _lock.lock();

            _timer.cancel();

            _taskWriter.interrupt();

            // Make sure we start mongo.
            if (!_mongoRunning.get()) startMongo();

            Thread.sleep(4000);

            System.out.println("Collection count: " + getCollection().getCount() + " - expecting: " + TEST_TASKS);

            final DBCursor cur = getCollection().find();

            final HashMap<Integer, Boolean> found = new HashMap<Integer, Boolean>();

            int inaccurateCount = 0;
            while (cur.hasNext()) {
                final BasicDBObject obj = (BasicDBObject)cur.next();
                found.put(obj.getInt("count"), Boolean.TRUE);
                if (!(obj.getString("test")).equals(TEST_DATA)) inaccurateCount++;
            }

            cur.close();

            int missingCount = 0;

            for (int idx=0; idx < TEST_TASKS; idx++) {
                final Boolean val = found.get(idx);
                if (val == null) missingCount++;
            }

            System.out.println("Missing : " + missingCount + " - inaccurate: " + inaccurateCount);

            // Stop mongo.
            stopMongo();
        } finally { _lock.unlock(); }
    }

    private class TaskWriter extends Thread {
        public void run() {

            while (true) {
                BasicDBObject task = null;
                try {
                    _lock.lock();

                    if (!_mongoRunning.get()) {
                        Thread.sleep(1000);
                        continue;
                    }

                    if (_tasks.isEmpty()) break;

                    task = _tasks.take();

                    final WriteResult result
                    = getCollection().insert(task, WriteConcern.FSYNC_SAFE);

                    final CommandResult cmdResult = result.getLastError();
                    if (!cmdResult.ok()) _tasks.put(task);

                } catch (final MongoException me) {

                    try { if (task != null) _tasks.put(task);
                    } catch (final InterruptedException ie) { break; }

                } catch (final IOException ioe) {

                    try { if (task != null) _tasks.put(task);
                    } catch (final InterruptedException ie) { break; }

                } catch (final InterruptedException ie) { break;
                } catch (final Throwable t) {

                    try { if (task != null) _tasks.put(task);
                    } catch (final InterruptedException ie) { break; }

                    //System.out.println("----------------------- throwable");

                    //t.printStackTrace();
                } finally { _lock.unlock(); }
            }
        }
    }

    private DBCollection getCollection() throws Exception
    { return getMongo().getDB("test").getCollection("durability"); }

    private Mongo getMongo() throws Exception {
        if (_mongo != null) return _mongo;
        _mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "test"));
        return getMongo();
    }

    private void forceSync() throws Exception
    { getMongo().getDB("admin").command("{fsync:1,async:true}"); }

    private void startMongo() throws Exception {
        final File lockFile = new File("/data/db/", "mongod.lock");
        if (lockFile.exists()) lockFile.delete();
        _mongoProcess = Runtime.getRuntime().exec(new String [] { "mongod", "--dur", "--logappend", "--logpath", "/tmp/mongo-dur.log", "--syncdelay", "120"  }, null, null);
        _mongoRunning.set(true);
    }

    private void stopMongo() throws Exception {
        _mongoRunning.set(false);
        if (_mongoProcess != null) { _mongoProcess.destroy(); _mongoProcess = null; }
        final File lockFile = new File("/data/db/", "mongod.lock");
        if (lockFile.exists()) lockFile.delete();
    }

    private void killMongo() throws Exception {
        _mongoRunning.set(false);
        Thread.sleep(1000);

        _mongo = null;
        if (_mongoProcess != null) {
            //forceSync();
            Runtime.getRuntime().exec(new String [] { "killall", "-9", "mongod" }, null, null);
           _mongoProcess.destroy();
           _mongoProcess = null;
        }

        final File lockFile = new File("/data/db/", "mongod.lock");
        if (lockFile.exists()) lockFile.delete();
    }

    private class MongoRunnerKillerTask extends TimerTask {
        public void run() {
            try {
                _lock.lock();

                // If running, we're going to kill it; otherwise, start Mongo.
                if (_mongoRunning.get()) killMongo();
                else startMongo();

            } catch (final Throwable t) { t.printStackTrace();
            } finally { _lock.unlock(); }
        }
    }

    private static final String TEST_DATA = "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

}

