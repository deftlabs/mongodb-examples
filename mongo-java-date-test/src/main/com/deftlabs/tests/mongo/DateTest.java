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
import com.mongodb.DBCollection;
import com.mongodb.DBAddress;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;

// JUnit
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

// Java
import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Calendar;
import java.text.SimpleDateFormat;

/**
 * A test to see if string comparison on a date is faster than using the date object.
 */
public final class DateTest {

    private static final int TEST_ITEMS = 10000;
    private static final int TEST_LOOKUPS = 10000;

    private Mongo _mongo;

    @Before
    public void setupMongo() throws Exception {
        _mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "test"));

        final BasicDBObject idx0 = new BasicDBObject("date", 1);
        idx0.put("name", 1);
        getCollection("date0").ensureIndex(idx0, "exampleIdx0", false);

        final BasicDBObject idx1 = new BasicDBObject("date", 1);
        idx1.put("name", 1);
        getCollection("date1").ensureIndex(idx1, "exampleIdx1", false);

        final BasicDBObject idx2 = new BasicDBObject("date", 1);
        idx2.put("name", 1);
        getCollection("date2").ensureIndex(idx2, "exampleIdx2", false);


        // Load some sample data.
        for (int idx=0; idx < TEST_ITEMS; idx++) {

            final Calendar cal = Calendar.getInstance();
            cal.add(Calendar.HOUR_OF_DAY, (idx * -1));

            final BasicDBObject item0 = new BasicDBObject("date", cal.getTime());
            item0.put("name", Integer.toString(idx));
            getCollection("date0").insert(item0);

            final BasicDBObject item1 = new BasicDBObject("date", cal.getTime().getTime());
            item1.put("name", Integer.toString(idx));
            getCollection("date1").insert(item1);

            final BasicDBObject item2 = new BasicDBObject("date", getDateStr(cal));
            item2.put("name", Integer.toString(idx));
            getCollection("date2").insert(item2);
        }

        Thread.sleep(5000);
    }

    @Test
    public void testPerformance() {


        final Calendar cal = Calendar.getInstance();
        final long time = cal.getTime().getTime();
        final Date date = cal.getTime();
        final String dateStr = getDateStr(cal);

        // Look at date0
        long startTime = System.currentTimeMillis();
        for (int idx=0; idx < TEST_LOOKUPS; idx++) {
            final BasicDBObject query = new BasicDBObject("date", new BasicDBObject("$gte", date));
            final DBCursor cur = getCollection("date0").find(query);
            while (cur.hasNext()) cur.next();
            cur.close();

        }

        long execTime = System.currentTimeMillis() - startTime;
        System.out.println("date0 exec time: " + execTime + " (ms)");

        // Look at date1
        startTime = System.currentTimeMillis();
        for (int idx=0; idx < TEST_LOOKUPS; idx++) {
            final BasicDBObject query = new BasicDBObject("date", new BasicDBObject("$gte", time));
            final DBCursor cur = getCollection("date1").find(query);
            while (cur.hasNext()) cur.next();
            cur.close();
        }

        execTime = System.currentTimeMillis() - startTime;
        System.out.println("date1 exec time: " + execTime + " (ms)");

        // Look at date1
        startTime = System.currentTimeMillis();
        for (int idx=0; idx < TEST_LOOKUPS; idx++) {
            final BasicDBObject query = new BasicDBObject("date", new BasicDBObject("$gte", dateStr));
            final DBCursor cur = getCollection("date2").find(query);
            while (cur.hasNext()) cur.next();
            cur.close();
        }

        execTime = System.currentTimeMillis() - startTime;
        System.out.println("date2 exec time: " + execTime + " (ms)");
    }

    /* Mongo */

    private DBCollection getCollection(final String pName) { return _mongo.getDB("test").getCollection(pName); }

    /* Time */

    public static String getDateStr(final Calendar pTime) { return getDateStr(pTime.getTime()); }

    private static String getDateStr(final Date pDate) { return ((SimpleDateFormat)_docTimeFormatter.get()).format(pDate); }

    private static final TimeZone TIME_ZONE = TimeZone.getTimeZone("GMT");

    private static ThreadLocal _docTimeFormatter = new ThreadLocal() {
        protected synchronized Object initialValue() {
            final SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            df.setTimeZone(TIME_ZONE); return df;
        }
    };
}

