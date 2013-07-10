package cmusv.ThermoRecommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.bson.types.ObjectId;

import junit.framework.TestCase;

public class GlobalRecommenderTest extends TestCase {

    public void testArticlePopularityCount(){
        MongoAdapter dbAdapter = new MongoAdapter();
        MahoutRecommenderAdapter mahoutAdapter = new MahoutRecommenderAdapter(dbAdapter);
        GlobalRecommender pc = new GlobalRecommender(mahoutAdapter.createDBModel(), dbAdapter);
        
    }
    public void testRecommendation() throws TasteException{
        MongoAdapter dbAdapter = new MongoAdapter();
        MahoutRecommenderAdapter mahoutAdapter = new MahoutRecommenderAdapter(dbAdapter);
        GlobalRecommender pc = new GlobalRecommender(mahoutAdapter.createDBModel(), dbAdapter);
        pc.recommend(new ObjectId("51cf362ee4b08233e8167fcc").hashCode(), 20);
    }
}
