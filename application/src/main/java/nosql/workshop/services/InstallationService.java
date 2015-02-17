package nosql.workshop.services;

import com.google.inject.Inject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import org.jongo.Aggregate;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Service permettant de manipuler les installations sportives.
 */
public class InstallationService {

    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    /**
     * Retourne une installation étant donné son numéro.
     *
     * @param numero le numéro de l'installation.
     * @return l'installation correspondante, ou <code>null</code> si non trouvée.
     */
    public Installation get(String numero) {
        return installations.findOne("{_id: # }",Integer.parseInt(numero)).as(Installation.class);
    }

    /**
     * Retourne la liste des installations.
     *
     * @param page     la page à retourner.
     * @param pageSize le nombre d'installations par page.
     * @return la liste des installations.
     */
    public List<Installation> list(int page, int pageSize) {

        Iterator<Installation> all = installations.find().skip((page - 1) * pageSize).limit(pageSize).as(Installation.class).iterator();
        List<Installation> list = new ArrayList<>();
        while (all.hasNext()){
            list.add(all.next());
        }
        return list;
    }

    /**
     * Retourne une installation aléatoirement.
     *
     * @return une installation.
     */
    public Installation random() {
        long count = count();
        int random = new Random().nextInt((int) count);

        return installations.find().skip(random).limit(1).as(Installation.class).iterator().next();
    }

    /**
     * Retourne le nombre total d'installations.
     *
     * @return le nombre total d'installations
     */
    public long count() {
        return installations.count();
    }

    /**
     * Retourne l'installation avec le plus d'équipements.
     *
     * @return l'installation avec le plus d'équipements.
     */
    public Installation installationWithMaxEquipments() {
        return  installations.aggregate("{ $group: { _id:'$_id', countEq:{$sum:1}}},{$sort:{'countEq':-1}},{$limit:1}").as(Installation.class).iterator().next();
    }

    /**
     * Compte le nombre d'installations par activité.
     *
     * @return le nombre d'installations par activité.
     */
    public List<CountByActivity> countByActivity() {

        Iterator<Installation> all = installations.aggregate("{$unwind : '$equipements'}").and("{$unwind : '$activites'}").as(Installation.class).iterator();
        List<CountByActivity> ret = new ArrayList<>();
        Map<String, CountByActivity> map = new HashMap<>();

        while (all.hasNext()){
            Installation ins = all.next();
            if (map.containsKey(ins.getEquipements().get(0).getNom())){
                CountByActivity ca = new CountByActivity();
                ca.setActivite(ins.getEquipements().get(0).getNom());
                ca.setTotal(1);
                map.put(ins.getEquipements().get(0).getNom(),ca);
            }else{
                CountByActivity ca = map.get(ins.getEquipements().get(0).getNom());
                ca.setTotal(ca.getTotal()+1);
            }
        }
        ret.addAll(map.values());
        return ret;
    }

    public double averageEquipmentsPerInstallation() {
        long ret = installations.aggregate("{$unwind : '$equipements' }").as(Object.class).size();
        return ret / this.count();

    }

    /**
     * Recherche des installations sportives.
     *
     * @param searchQuery la requête de recherche.
     * @return les résultats correspondant à la requête.
     */
    public List<Installation> search(String searchQuery) {
        Iterator<Installation> all = installations.find("{nom: #}", searchQuery).as(Installation.class).iterator();
        List<Installation> list = new ArrayList<>();
        while (all.hasNext()){
            list.add(all.next());
        }
        return list;

    }

    /**
     * Recherche des installations sportives par proximité géographique.
     *
     * @param lat      latitude du point de départ.
     * @param lng      longitude du point de départ.
     * @param distance rayon de recherche.
     * @return les installations dans la zone géographique demandée.
     */
    public List<Installation> geosearch(double lat, double lng, double distance) {


        DBObject index2d = BasicDBObjectBuilder.start("location", "2dsphere").get();
        installations.getDBCollection().createIndex(index2d);

        MongoCursor<Installation> it = installations.find("{ location : { $near :{ $geometry :{ type : 'Point' ,coordinates : [ "+lng+", "+lat+" ]},$maxDistance : " + distance + "}}}").as(Installation.class);

        List<Installation> all = new ArrayList<>();
        while (it.hasNext()){
            all.add(it.next());
        }
        return all;

    }

}
