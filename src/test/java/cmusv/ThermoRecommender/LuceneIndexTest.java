package cmusv.ThermoRecommender;

import junit.framework.TestCase;

public class LuceneIndexTest extends TestCase {
    private MongoAdapter dbAdapter;
    private LuceneIndexer indexer; 
    public LuceneIndexTest(){
        dbAdapter = new MongoAdapter();
        indexer = new LuceneIndexer();
    }
    public void testIndexer() throws Throwable{
        indexer.mongoToLucene(dbAdapter);
    }
    public void testLDA() throws Throwable{
        indexer.runLDA();
        
    }
}
