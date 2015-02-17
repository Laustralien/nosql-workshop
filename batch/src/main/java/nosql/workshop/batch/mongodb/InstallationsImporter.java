package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Importe les 'installations' dans MongoDB.
 */
public class InstallationsImporter {

    private final DBCollection installationsCollection;


    public InstallationsImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
    }

    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/installations.csv");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> installationsCollection.save(toDbObject(line)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DBObject toDbObject(final String line) {
        String[] columns = line.substring(1, line.length() - 1).split("\",\"");

        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("_id", columns[1]);
        dbObject.put("nom", columns[0]);

        BasicDBObject adresseObject = new BasicDBObject();
        adresseObject.put("numero", columns[6]);
        adresseObject.put("voie", columns[7]);
        adresseObject.put("lieuDit", columns[5]);
        adresseObject.put("codePostal",columns[4] );
        adresseObject.put("commune", columns[2]);
        dbObject.put("adresse", adresseObject);

        BasicDBObject locationObject = new BasicDBObject();
        locationObject.put("type", "Point");
        double[] locations = new double[2];
        locations[0] = Double.parseDouble(columns[9]);
        locations[1] = Double.parseDouble(columns[10]);
        locationObject.put("coordinates", locations);
        dbObject.put("location",locationObject);


        try {
            dbObject.put("multiCommune", Boolean.parseBoolean(columns[16]));
        } catch (NumberFormatException e) {
            dbObject.put("multiCommune",0);
        }
        try {
            dbObject.put("nbPlacesParking", Integer.parseInt(columns[17]));
        } catch (NumberFormatException e) {
            dbObject.put("nbPlacesParking", 0);
        }
        try {
            dbObject.put("nbPlacesParkingHandicapes", Integer.parseInt(columns[18]));
        } catch (NumberFormatException e) {
            dbObject.put("nbPlacesParkingHandicapes", 0);
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MMM-dd");


        try {
            Date date = formatter.parse(columns[28]);
            dbObject.put("dateMiseAJourFiche", date);
        } catch (ParseException | ArrayIndexOutOfBoundsException e) {
            dbObject.put("dateMiseAJourFiche", new Date());
        }


        return dbObject;
    }
}
