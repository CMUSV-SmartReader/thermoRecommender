package cmusv.ThermoRecommender;

import junit.framework.TestCase;

public class MongoAdapterTest extends TestCase {
    protected MongoAdapter reader;
    public MongoAdapterTest(String testName){
        super(testName);
        reader = new MongoAdapter("54.215.148.76", 27017, "thermoreader", null, null);
    }
    
    public void testConnection()
    {
        assertTrue(reader.makeConnection());
    }
    public void testGetArticles() {
        reader.makeConnection();
        reader.getArticles();
    }
}

