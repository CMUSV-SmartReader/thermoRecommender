package cmusv.ThermoRecommender;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    private static Logger logger;
    public static Logger getLogger(){
        if(logger==null) 
            logger = LoggerFactory.getLogger(App.class); 
        return logger;
    }
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
