package cmusv.ThermoRecommender;

import junit.framework.TestCase;

public class MongoAdapterTest extends TestCase {
    protected MongoAdapter reader;
    public MongoAdapterTest(String testName){
        super(testName);
        reader = new MongoAdapter("test.lydian.tw", 27017, "thermoreader-test", null, null);
    }
    
    public void testConnection()
    {
        //assertTrue(reader.makeConnection());
    }
    public void testGetArticles() {
        reader.getArticles();
    }
    public void testGetArticleIds(){
        System.out.println(reader.getLatestArticleIds().size());
    }
}

