package cmusv.ThermoRecommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.RandomRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import junit.framework.TestCase;

public class MahoutRecommenderTest extends TestCase {
    MahoutRecommenderAdapter adapter;
    public MahoutRecommenderTest(){
        MongoAdapter reader = new MongoAdapter("ec2.lydian.tw", 27017, "thermoreader-test", null, null);
        adapter = new MahoutRecommenderAdapter(reader);
    }
    public void testGetDB(){
        adapter.createDBModel();
    }
    public void testRandomRecommendation() throws TasteException{
        Recommender recommender = new RandomRecommender(adapter.createDBModel());
        adapter.setRecommender(recommender);
        adapter.recommendArticles(40);
    }
    public void testGenericRecommendation() throws TasteException{
        DataModel dbm = adapter.createDBModel();
        UserSimilarity userSim = new PearsonCorrelationSimilarity(dbm);
        Recommender recommender = new GenericUserBasedRecommender(dbm, new NearestNUserNeighborhood(10, userSim, dbm),userSim);
        adapter.setRecommender(recommender);
        adapter.recommendArticles(40);
    }
}
