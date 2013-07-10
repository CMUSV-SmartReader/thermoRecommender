package cmusv.ThermoRecommender;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        MongoAdapter mongo = new MongoAdapter();
        MahoutRecommenderAdapter adapter = new MahoutRecommenderAdapter(mongo);
       
        DataModel dbm = adapter.createDBModel();
        Recommender recommender = new GlobalRecommender(adapter.createDBModel(), mongo);
        adapter.setRecommender(recommender);
        adapter.recommendArticles(40);
    }
}
