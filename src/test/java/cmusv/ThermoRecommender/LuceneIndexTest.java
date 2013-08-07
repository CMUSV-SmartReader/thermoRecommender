package cmusv.ThermoRecommender;

import java.io.IOException;

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
    public void testLuceneToSequence() throws Throwable {
        indexer.luceneToSequence();
        indexer.sequenceToVector();
        
    }
    public void testLDA() throws Throwable{
        indexer.runLDA();
        
    }
    
    public void testShowTopics() throws Throwable{
        //indexer.getTopics();
        dbAdapter.setDuplicateArticles(indexer.getTopics());
        
    }
    public void tearDown(){
        System.out.println("AFTER");
        
    }
}
