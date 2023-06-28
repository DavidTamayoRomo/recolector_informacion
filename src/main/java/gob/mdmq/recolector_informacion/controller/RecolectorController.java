package gob.mdmq.recolector_informacion.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;

import gob.mdmq.recolector_informacion.model.Recolector;
import gob.mdmq.recolector_informacion.repository.RecolectorRepository;

@RestController
@RequestMapping("/recolectar")
public class RecolectorController {

    private final RecolectorRepository recolectorRepository;
    private final MongoOperations mongoOperations;
    private final MongoTemplate mongoTemplate;

    @Autowired
    public RecolectorController(RecolectorRepository recolectorRepository, MongoOperations mongoOperations,
            MongoTemplate mongoTemplate) {
        this.recolectorRepository = recolectorRepository;
        this.mongoOperations = mongoOperations;
        this.mongoTemplate = mongoTemplate;
    }

    @GetMapping("/colecciones")
    public List<Document> colecciones() {
        try {

            Recolector recolectar = obtenerUltimoRegistro();
            if (recolectar.getColeccion_actual() != null) {

                Recolector ultimoRegistro = obtenerUltimoRegistro();
                long totalDatos = ultimoRegistro.getTotal_datos();
                long recorridos = ultimoRegistro.getRecorridos();

                if (recorridos >= totalDatos) {
                    System.out.println("1. No hay mas datos, ir al siguiente registro");
                    List<String> collectionName = obtenerColecciones();
                    int indice = collectionName.indexOf(ultimoRegistro.getColeccion_actual());
                    String coleccion_actual = collectionName.get(indice + 1);
                    String coleccion_siguiente = collectionName.get(indice + 2);

                    FindIterable<Document> resultado = mongoOperations
                            .getCollection(coleccion_actual).find().skip(0)
                            .limit(5000);
                    List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                            .collect(Collectors.toList());
                    Recolector recolector = new Recolector();
                    recolector.setColeccion_actual(coleccion_actual);
                    recolector.setColeccion_siguiente(coleccion_siguiente);
                    recolector.setTotal_datos(mongoOperations.getCollection(coleccion_actual).countDocuments());
                    recolector.setRecorridos(5000);
                    recolector.setFecha(new Date());
                    recolectorRepository.save(recolector);

                    List<Document> datosList = new ArrayList<>();
                    for (Document document : documentList) {
                        Document datos = (Document) document.get("datos");
                        datosList.add(datos);
                    }

                    return datosList;
                } else {
                    System.out.println("2. Hay mas datos, continuar en la misma coleccion");

                    Integer recorrido = (int) recorridos;

                    FindIterable<Document> resultado = mongoOperations
                            .getCollection(ultimoRegistro.getColeccion_actual()).find()
                            .skip((int) ultimoRegistro.getRecorridos())
                            .limit(5000);

                    List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                            .collect(Collectors.toList());

                    Recolector recolector = new Recolector();
                    recolector.setColeccion_actual(ultimoRegistro.getColeccion_actual());
                    recolector.setColeccion_siguiente(ultimoRegistro.getColeccion_siguiente());
                    recolector.setTotal_datos(
                            mongoOperations.getCollection(ultimoRegistro.getColeccion_actual()).countDocuments());
                    recolector.setRecorridos(recorrido + 5000);
                    recolector.setFecha(new Date());
                    recolectorRepository.save(recolector);

                    List<Document> datosList = new ArrayList<>();
                    for (Document document : documentList) {
                        Document datos = (Document) document.get("datos");
                        datosList.add(datos);
                    }

                    return datosList;
                }

            } else {
                System.out.println("No hay registros");
                List<String> collectionName = obtenerColecciones();
                String coleccion_actual = collectionName.get(0);
                String coleccion_siguiente = collectionName.get(1);
                FindIterable<Document> resultado = mongoOperations.getCollection(coleccion_actual).find().skip(0)
                        .limit(5000);

                List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                        .collect(Collectors.toList());
                Recolector recolector = new Recolector();
                recolector.setColeccion_actual(coleccion_actual);
                recolector.setColeccion_siguiente(coleccion_siguiente);
                recolector.setTotal_datos(mongoOperations.getCollection(coleccion_actual).countDocuments());
                recolector.setRecorridos(5000);
                recolector.setFecha(new Date());
                recolectorRepository.save(recolector);

                List<Document> datosList = new ArrayList<>();
                for (Document document : documentList) {
                    Document datos = (Document) document.get("datos");
                    datosList.add(datos);
                }

                return datosList;

            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            return null;
        }
    }

    public List<String> obtenerColecciones() {
        Set<String> databaseName = mongoOperations.getCollectionNames();
        List<String> lista = new ArrayList<>(databaseName);
        // Eliminar un elemento de la lista
        lista.remove("recolector");
        lista.remove("datos");
        return lista;
    }

    public Recolector obtenerUltimoRegistro() {
        Query query = new Query().limit(1).with(
                org.springframework.data.domain.Sort.by(org.springframework.data.domain.Sort.Direction.DESC, "fecha"));
        return mongoTemplate.findOne(query, Recolector.class);
    }

}
