package cmusv.ThermoRecommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.bson.types.ObjectId;

import junit.framework.TestCase;

public class GlobalRecommenderTest extends TestCase {
    private MongoAdapter dbAdapter;
    private MahoutRecommenderAdapter mahoutAdapter;
    GlobalRecommender recommender;
    public GlobalRecommenderTest(){
        dbAdapter = new MongoAdapter();
        mahoutAdapter = new MahoutRecommenderAdapter(dbAdapter);
        recommender = new GlobalRecommender(mahoutAdapter.createDBModel(), dbAdapter);;
    }
    
    public void testArticlePopularityCount(){
        System.out.println(recommender.countArticlePopularity());
    }
    public void testArticleScore(){
        System.out.println(recommender.createArticleScore());
    }
    public void testRecommendation() throws TasteException{
        recommender.recommend(new ObjectId("51cf362ee4b08233e8167fcc").hashCode(), 20);
    }
}
