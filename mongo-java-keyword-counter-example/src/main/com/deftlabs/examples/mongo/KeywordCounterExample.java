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
import com.mongodb.DBCollection;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

// JUnit
import org.junit.Test;
import static org.junit.Assert.*;

// Java
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.BufferedReader;
import java.util.List;
import java.util.ArrayList;
import java.util.zip.CRC32;

/**
 * This is an example of a hashing keyword counter. This is a generic example
 * but it should be tuned based on your dataset. In this example, you can
 * adjust the DOC_BUCKET_COUNT and DOC_COUNT. You could also modify the first/second
 * level hashing logic.
 *
 * Remember... once you pick an algorithm, you have to stick with it (or migrate your data).
 */
public final class KeywordCounterExample {

    /**
     * Run the test.
     */
    @Test
    public void testSimple() throws Exception {
        final Mongo mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017/test"));

        final DBCollection col =  mongo.getDB("test").getCollection("testKeywordCounter");
        col.drop();

        final ObjectId groupId = ObjectId.get();

        final List<String> words = readWords();

        //System.out.println("word count: " + words.size());

        for (final String word : words) hashAndUpdateCounter(word, "20111202", groupId, col);

    }

    private static final int DOC_BUCKET_COUNT = 100;
    private static final int DOC_COUNT = 50;

    // This is not thread safe... just easy for this example.
    private static final CRC32 _hasher = new CRC32();

    /**
     * Hash the words into X buckets.
     */
    private void hashAndUpdateCounter(  final String pWord,
                                        final String pDate,
                                        final ObjectId pGroupId,
                                        final DBCollection pCollection)
        throws Exception
    {
        _hasher.reset();

        final byte [] raw = pWord.getBytes("UTF-8");

        _hasher.update(raw);
        final long idHash = _hasher.getValue();

        // Get the document id.
        final String docId = pGroupId.toString() + "-" + pDate + "-" + (idHash % DOC_COUNT);

        //System.out.println(docId);

        _hasher.reset();
        _hasher.update(raw, 0, (raw.length > 6) ? 6 : raw.length);
        final long firstHash = _hasher.getValue();

        _hasher.reset();
        _hasher.update(raw, 0, (raw.length > 3) ? 3 : raw.length);
        final long secondHash = _hasher.getValue();

        // You need to be careful to escape periods... they are not allowed as keys.
        final String field = "keywords." + (firstHash % DOC_BUCKET_COUNT) + "." + (secondHash % DOC_BUCKET_COUNT) + "." + pWord;

        final BasicDBObject toSet = new BasicDBObject("groupId", pGroupId);
        toSet.put("date", pDate);

        final BasicDBObject vals = new BasicDBObject("$inc", new BasicDBObject(field, 1));
        vals.put("$set", toSet);

        pCollection.update(new BasicDBObject("_id", docId), vals, true, false);
    }

    /**
     * Do an simple/ugly split and pull out words (and everything else :-)).
     */
    private List<String> readWords() throws Exception {
        final DataInputStream dis = new DataInputStream(new FileInputStream("data/war_and_peace.txt"));
        final BufferedReader br = new BufferedReader(new InputStreamReader(dis));
        final ArrayList<String> words = new ArrayList<String>();

        boolean allOk = true;
        String line;
        while ((line = br.readLine()) != null) {
            for (String word : line.split(" ")) {
                if (word == null) continue;
                word = word.trim();
                if (word.equals("")) continue;
                allOk = true;
                for (final String ignore : IGNORE_CHARS)
                { if (word.indexOf(ignore) != -1) { allOk = false; break; } }
                if (!allOk) continue;

                words.add(word);
            }
        }

        dis.close();

        return words;
    }

    private static final String [] IGNORE_CHARS = { "(", ")", ".", "\"", "?" };
}

