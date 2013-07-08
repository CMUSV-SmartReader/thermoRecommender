package cmusv.ThermoRecommender;
import java.net.UnknownHostException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.mongodb.MongoDBDataModel;

import com.mongodb.MongoException;

import junit.framework.TestCase;


public class PreferenceDBModelTest extends TestCase {
    public void testGetItemIDs() throws TasteException, UnknownHostException, MongoException{
        MongoDBDataModel m = new MongoDBDataModel("ec2.lydian.tw", 27017,"thermoreader-test", "userArticle", false, false, null, "user", "article" );
        System.out.println(m.getNumItems());
        //MongoArticleReader reader = new MongoArticleReader("ec2.lydian.tw", 27017, "thermoreader-test", null, null);
        //PreferenceDBModel model = new PreferenceDBModel(reader);
        //System.out.println(model.getItemIDs());
    }
}
