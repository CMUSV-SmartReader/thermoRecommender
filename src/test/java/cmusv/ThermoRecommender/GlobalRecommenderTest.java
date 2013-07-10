package cmusv.ThermoRecommender;

import junit.framework.TestCase;

public class GlobalRecommenderTest extends TestCase {

    public void testArticlePopularityCount(){
        MongoAdapter dbAdapter = new MongoAdapter();
        MahoutRecommenderAdapter mahoutAdapter = new MahoutRecommenderAdapter(dbAdapter);
        GlobalRecommender pc = new GlobalRecommender(mahoutAdapter.createDBModel(), dbAdapter);
        
    }
}
