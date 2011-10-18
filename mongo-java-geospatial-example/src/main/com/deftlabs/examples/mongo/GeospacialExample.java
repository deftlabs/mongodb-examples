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
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

// JUnit
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

// Java
import java.util.List;
import java.util.LinkedList;

/**
 * An example of how to use a geospacial index.
 */
public final class GeospacialExample {

    private Mongo _mongo;

    @Before
    public void setupMongo() throws Exception {
        _mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "geospacial"));
        getCollection().ensureIndex(new BasicDBObject("loc", "2d"), "geospacialIdx");
        addVenues();
    }

    @Test
    public void nearWithMaxDistanceExample() {
        final BasicDBObject filter = new BasicDBObject("$near", new double[] { -73.99171, 40.738868 });
        filter.put("$maxDistance", 0.01);

        final BasicDBObject query = new BasicDBObject("loc", filter);

        int count = 0;
        for (final DBObject venue : getCollection().find(query).toArray()) {
            //System.out.println("---- near venue: " + venue.get("name"));
            count++;
        }

        assertEquals(count, 8);
    }

    @Test
    public void withinBoxExample() {
        final LinkedList<double[]> box = new LinkedList<double[]>();

        // Set the lower left point - Washington square park
        box.addLast(new double[] {  -73.99756, 40.73083 });

        // Set the upper right point - Flatiron Building
        box.addLast(new double[] { -73.988135, 40.741404 });

        final BasicDBObject query
        = new BasicDBObject("loc", new BasicDBObject("$within", new BasicDBObject("$box", box)));

        int count = 0;
        for (final DBObject venue : getCollection().find(query).toArray()) {
            //System.out.println("---- near venue: " + venue.get("name"));
            count++;
        }

        assertEquals(count, 5);
    }

    @Test
    public void withinPolygonExample() {

        final LinkedList<double[]> polygon = new LinkedList<double[]>();

        // Long then lat

        // Create the shape.
        polygon.addLast(new double[] {  -73.99756, 40.73083 });
        polygon.addLast(new double[] { -73.988135, 40.741404 });
        polygon.addLast(new double[] { -73.99171, 40.738868  });

        final BasicDBObject query
        = new BasicDBObject("loc", new BasicDBObject("$within", new BasicDBObject("$polygon", polygon)));

        int count = 0;
        for (final DBObject venue : getCollection().find(query).toArray()) {
            //System.out.println("---- near venue: " + venue.get("name"));
            count++;
        }

        assertEquals(count, 3);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void withinCircleExample() {
        final LinkedList circle = new LinkedList();

        // Set the center - 10gen Office
        circle.addLast(new double[] { -73.99171, 40.738868 });

        // Set the radius
        circle.addLast(0.01);

        final BasicDBObject query
        = new BasicDBObject("loc", new BasicDBObject("$within", new BasicDBObject("$center", circle)));

        int count = 0;
        for (final DBObject venue : getCollection().find(query).toArray()) {
            //System.out.println("---- near venue: " + venue.get("name"));
            count++;
        }

        assertEquals(count, 8);
    }

    @Test
    public void fast() {
        for (int idx=0; idx < 10000; idx++) nearSphereWIthMaxDistance();
    }


    @Test
    public void nearSphereWIthMaxDistance() {
        final BasicDBObject filter = new BasicDBObject("$nearSphere", new double[] { -73.99171, 40.738868 });
        //filter.put("$maxDistance", 0.002572851730235);
        //filter.put("$maxDistance", 0.0036998565637149016);
        filter.put("$maxDistance", 0.003712240453784);

        // Radius of the earth: 3959.8728
        // Distance to Maplewood, NJ (in radians): 0.0036998565637149016
        // Distance to Maplewood, NJ (in miles 0.0036998565637149016  * 3959.8728): 14.65

        //db.example.find( { loc: { $nearSphere: [ -73.99171, 40.738868 ], $maxDistance: 0.0036998565637149016 }});

        // To get a list of all places (with distance in radians):
        //db.runCommand( { geoNear : "example" , near : [-73.99171,40.738868], spherical: true } );
        //db.runCommand( { geoNear : "example" , near : [-73.99171,40.738868], maxDistance : 0.0036998565637149016, spherical: true } );

        final BasicDBObject query = new BasicDBObject("loc", filter);

        int count = 0;
        for (final DBObject venue : getCollection().find(query).toArray()) {
            //System.out.println("---- near venue: " + venue.get("name"));
            count++;
        }

        assertEquals(count, 11);
    }

    private void addVenues() {
        addVenue("10gen Office",  new double[] { -73.99171, 40.738868  });
        addVenue("Flatiron Building", new double[] { -73.988135, 40.741404 });
        addVenue("Players Club", new double[] { -73.997812, 40.739128 });
        addVenue("City Bakery ", new double[] { -73.992491, 40.738673 });
        addVenue("Splash Bar", new double[] { -73.992491, 40.738673 });
        addVenue("Momofuku Milk Bar", new double[] { -73.985839, 40.731698 });
        addVenue("Shake Shack", new double[] { -73.98820, 40.74164 });
        addVenue("Penn Station", new double[] {  -73.99408, 40.75057 });
        addVenue("Empire State Building", new double[] { -73.98602, 40.74894 });
        addVenue("Washington Square Park", new double[] { -73.99756, 40.73083 });
        addVenue("Ulaanbaatar, Mongolia", new double[] { 106.9154, 47.9245 });
        addVenue("Maplewood, NJ", new double[] { -74.2713, 40.73137 });
    }

    private void addVenue(  final String pName,
                            final double [] pLocation)
    {
        final BasicDBObject loc = new BasicDBObject("name", pName);
        loc.put("loc", pLocation);
        getCollection().update(new BasicDBObject("name", pName), loc, true, false);
    }

    private DBCollection getCollection() {
        return _mongo.getDB("geospacial").getCollection("example");
    }
}

