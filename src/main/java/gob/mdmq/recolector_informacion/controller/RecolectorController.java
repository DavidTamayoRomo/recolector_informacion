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
                /* System.out.println("Ultimo registro: " + recolectar);
                List<String> collectionName = obtenerColecciones();
                String coleccion = collectionName.get(collectionName.size() - 1);
                System.out.println("Coleccion 1: " + coleccion); */

                Recolector ultimoRegistro = obtenerUltimoRegistro();
                long totalDatos = ultimoRegistro.getTotal_datos();
                long recorridos = ultimoRegistro.getRecorridos();

                if (recorridos >= totalDatos) {
                    System.out.println("1. No hay mas datos, ir al siguiente registro");

                    //ver como poner el siguiente
                    List<String> collectionName = obtenerColecciones();
                     //encontar el indice de la coleccion actual
                    int indice = collectionName.indexOf(ultimoRegistro.getColeccion_actual());
                    String coleccion_actual = collectionName.get(indice + 1);
                    String coleccion_siguiente = collectionName.get(indice + 2);

                    FindIterable<Document> resultado = mongoOperations
                            .getCollection(coleccion_actual).find().skip(0)
                            .limit(5000);
                    List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                            .collect(Collectors.toList());

                    

                    // Almacenar en la base de datos
                    
                    Recolector recolector = new Recolector();
                    recolector.setColeccion_actual(coleccion_actual);
                    recolector.setColeccion_siguiente(coleccion_siguiente);
                    recolector.setColeccion_anterior("");
                    recolector.setTotal_datos(mongoOperations.getCollection(coleccion_actual).countDocuments());
                    recolector.setRecorridos(5000);
                    recolector.setFecha(new Date());
                    recolectorRepository.save(recolector);

                    return documentList;
                } else {
                    System.out.println("2. Hay mas datos, continuar en la misma coleccion");

                    Integer recorrido = (int) recorridos;


                    FindIterable<Document> resultado = mongoOperations
                            .getCollection(ultimoRegistro.getColeccion_actual()).find().skip((int)ultimoRegistro.getRecorridos())
                            .limit(5000);

                    List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                            .collect(Collectors.toList());

                    Recolector recolector = new Recolector();
                    recolector.setColeccion_actual(ultimoRegistro.getColeccion_actual());
                    recolector.setColeccion_siguiente(ultimoRegistro.getColeccion_siguiente());
                    recolector.setColeccion_anterior(ultimoRegistro.getColeccion_anterior());
                    recolector.setTotal_datos(mongoOperations.getCollection(ultimoRegistro.getColeccion_actual()).countDocuments());
                    recolector.setRecorridos(recorrido + 5000);
                    recolector.setFecha(new Date());
                    recolectorRepository.save(recolector);

                    return documentList;
                }

            } else {
                System.out.println("No hay registros");
                // Almacenar por primera vez
                // ver todas las colecciones de la base de datos
                List<String> collectionName = obtenerColecciones();
                String coleccion_actual = collectionName.get(0);
                String coleccion_siguiente = collectionName.get(1);

                // --------------------------------

                // Obtener los primeros 5000 datos de la coleccion con el nombre
                // coleccion_actual y skip 0
                FindIterable<Document> resultado = mongoOperations.getCollection(coleccion_actual).find().skip(0)
                        .limit(5000);

                List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                        .collect(Collectors.toList());

                // Almacenar en la base de datos
                Recolector recolector = new Recolector();
                recolector.setColeccion_actual(coleccion_actual);
                recolector.setColeccion_siguiente(coleccion_siguiente);
                recolector.setColeccion_anterior("");
                recolector.setTotal_datos(mongoOperations.getCollection(coleccion_actual).countDocuments());
                recolector.setRecorridos(5000);
                recolector.setFecha(new Date());
                recolectorRepository.save(recolector);

                return documentList;

            }

        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Error: " + e.getMessage());
            return null;
        }
    }

    public List<String> obtenerColecciones() {
        Set<String> databaseName = mongoOperations.getCollectionNames();
        List<String> lista = new ArrayList<>(databaseName);
        return lista;
    }

    public Recolector obtenerUltimoRegistro() {
        Query query = new Query().limit(1).with(
                org.springframework.data.domain.Sort.by(org.springframework.data.domain.Sort.Direction.DESC, "fecha"));
        return mongoTemplate.findOne(query, Recolector.class);
    }

}
