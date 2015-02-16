package nosql.workshop.batch.mongodb;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * Importe les données issues des fichiers CSV dans la base MongoDB.
 */
public class CsvToMongoDb {

    public static void main(String[] args) {
        MongoClient mongoClient = null;
        try {
            // Création du client Mongo
            mongoClient = new MongoClient("localhost" , 27017);

            // Récupération de la collection "installations" de la base "nosql-workshop"
            DB db = mongoClient.getDB("nosql-workshop");
            DBCollection installationsCollection = db.getCollection("installations");
            installationsCollection.drop();

            // Import des données
            new InstallationsImporter(installationsCollection).run();
            System.out.println("installations Ok");

            new EquipementsImporter(installationsCollection).run();
            System.out.println("equipements Ok");

            new ActivitesImporter(installationsCollection).run();
            System.out.println("activites Ok");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

}
