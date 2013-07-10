package cmusv.ThermoRecommender;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        MongoAdapter mongo = new MongoAdapter();
        MahoutRecommenderAdapter recommender = new MahoutRecommenderAdapter(mongo);
        recommender.recommendArticles(20);
    }
}
