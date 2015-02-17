package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class EquipementsImporter {

    private final DBCollection installationsCollection;
    private HashMap<String, DBObject> installs ;

    public EquipementsImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
        installs = new HashMap<String, DBObject>();
    }

    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/equipements.csv");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> updateInstallation(line));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void updateInstallation(final String line) {
        String[] columns = line.split(",");

        DBObject toEdit ;
        BasicDBObject query = new BasicDBObject();
        query.put("_id", columns[2]);

        if (!installs.containsKey(columns[2])) {
            toEdit = this.installationsCollection.findOne(query);
            installs.put(columns[2], toEdit);
        }else{
            toEdit = installs.get(columns[2]);
        }
        toEdit.put("dateMiseAJourFiche", new Date());

        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("numero",columns[4]);
        dbObject.put("nom", columns[4]);
        dbObject.put("type", columns[7]);
        dbObject.put("famille", columns[8]);

        DBObject listItem = new BasicDBObject("equipements", dbObject);
        DBObject updateQuery = new BasicDBObject("$push", listItem);
        installationsCollection.update( query, updateQuery );

    }
}
