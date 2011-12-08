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
import com.mongodb.*;
import org.bson.types.*;

// Morphia
import com.google.code.morphia.*;
import com.google.code.morphia.annotations.*;

// JUnit
import org.junit.*;
import static org.junit.Assert.*;

/**
 * Examples of using morphia. This assumes you are running mongo on 127.0.0.1:27017
 */
public final class MorphiaExample {

    /**
     * A simple insert write concern.
     */
    @Test
    public void morphiaExample() throws Exception {

        final Morphia morphia = new Morphia();
        morphia.mapPackage("com.deftlabs.examples.mongo");

        final Datastore datastore
        = morphia.createDatastore(new Mongo(new MongoURI("mongodb://127.0.0.1:27017")), "mongo-java-driver-intro");

        // Create the object(s)
        final ObjectId docId = ObjectId.get();

        final TestEntity test = new TestEntity();
        test.setId(docId);
        test.setName("NameValueTest");

        final Child child = new Child();
        child.setChildName("NameValueTestChild");
        test.setChild(child);

        // Save
        datastore.save(test);

        // Query for the entity.
        final TestEntity findTest = datastore.find(TestEntity.class, "_id", docId).get();
        assertNotNull(findTest);

        assertNotNull(findTest.getChild());
    }

    /**
     * Define the test entity.
     */
    @Entity(value="morphiaExamples", noClassnameStored=true, slaveOk=false)
    private static class TestEntity {
        @Id
        private ObjectId _id;

        @Property("name")
        private String _name;

        @Embedded("nested")
        private Child _child;

        public void setId(final ObjectId pV) { _id = pV; }
        public ObjectId getId() { return _id; }

        public void setName(final String pV) { _name = pV; }
        public String getName() { return _name; }

        public void setChild(final Child pV) { _child = pV; }
        public Child getChild() { return _child; }
    }

    /**
     * Define the nested doc.
     */
    @Embedded
    private static class Child {
        @Property("name")
        private String _childName;

        public void setChildName(final String pV) { _childName = pV; }
        public String getChildName() { return _childName; }

    }
}

