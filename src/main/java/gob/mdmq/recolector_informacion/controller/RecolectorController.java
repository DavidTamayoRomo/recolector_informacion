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
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
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

                Recolector ultimoRegistro = obtenerUltimoRegistro();
                long totalDatos = ultimoRegistro.getTotal_datos();
                long recorridos = ultimoRegistro.getRecorridos();

                if (recorridos >= totalDatos) {
                    System.out.println("1. No hay mas datos, ir al siguiente registro");
                    List<String> collectionName = obtenerColecciones();
                    int numeroColecciones = collectionName.size();

                    int indice = collectionName.indexOf(ultimoRegistro.getColeccion_actual());

                    int indiceActual = indice + 1;
                    int indiceSiguiente = indice + 2;

                    if (indiceSiguiente >= numeroColecciones) {
                        indiceSiguiente = 0;
                    }
                    if (indiceActual >= numeroColecciones) {
                        indiceActual = 0;
                        indiceSiguiente = 1;
                    }
                    String coleccion_actual = collectionName.get(indiceActual);
                    String coleccion_siguiente = collectionName.get(indiceSiguiente);

                    FindIterable<Document> resultado = mongoOperations
                            .getCollection(coleccion_actual).find().skip(0)
                            .limit(5000);
                    List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                            .collect(Collectors.toList());
                    Recolector recolector = new Recolector();
                    recolector.setColeccion_actual(coleccion_actual);
                    recolector.setColeccion_siguiente(coleccion_siguiente);
                    recolector.setTotal_datos(mongoOperations.getCollection(coleccion_actual).countDocuments());
                    recolector.setRecorridos(documentList.size());
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
                    recolector.setRecorridos(recorrido + documentList.size());
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
                recolector.setRecorridos(documentList.size());
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

    @GetMapping("/colecciones2")
    public List<Document> colecciones2() {
        try {

            Recolector recolectar = obtenerUltimoRegistro();

            if (recolectar.getColeccion_actual() != null) {
                // existe registros en la base de datos
                // Verificar el ultimo registro de la coleccion
                Recolector ultimoRegistroColeccion = obtenerUltimoRegistroPorColeccion(
                        recolectar.getColeccion_actual());
                long totalDatosColeccion = ultimoRegistroColeccion.getTotal_datos();
                long recorridosColeccion = ultimoRegistroColeccion.getRecorridos();

                // Obtener el total de datos de la coleccion
                long totalColeccion = mongoOperations.getCollection(recolectar.getColeccion_actual()).countDocuments();

                if (totalColeccion > totalDatosColeccion) {
                    // Existen nuevos registros en la coleccion
                    Recolector ultimoRegistro = obtenerUltimoRegistro();
                    long recorridos = ultimoRegistro.getRecorridos();
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
                    recolector.setRecorridos(recorrido + documentList.size());
                    recolector.setFecha(new Date());
                    recolectorRepository.save(recolector);

                    List<Document> datosList = new ArrayList<>();
                    for (Document document : documentList) {
                        Document datos = (Document) document.get("datos");
                        datosList.add(datos);
                    }

                    return datosList;
                } else {
                    // No existen nuevos registros en la coleccion
                    Recolector ultimoRegistro = obtenerUltimoRegistro();
                    long totalDatos = ultimoRegistro.getTotal_datos();
                    long recorridos = ultimoRegistro.getRecorridos();

                    if (recorridos >= totalDatos) {
                        System.out.println("1. No hay mas datos, ir al siguiente registro");

                        Recolector ultimoRegistroColeccionSig = obtenerUltimoRegistroPorColeccion(
                                recolectar.getColeccion_siguiente());

                        List<String> collectionName = obtenerColecciones();
                        int numeroColecciones = collectionName.size();

                        int indice = collectionName.indexOf(ultimoRegistro.getColeccion_actual());

                        int indiceActual = indice + 1;
                        int indiceSiguiente = indice + 2;

                        if (indiceSiguiente >= numeroColecciones) {
                            indiceSiguiente = 0;
                        }
                        if (indiceActual >= numeroColecciones) {
                            indiceActual = 0;
                            indiceSiguiente = 1;
                        }
                        String coleccion_actual = collectionName.get(indiceActual);
                        String coleccion_siguiente = collectionName.get(indiceSiguiente);

                        if (ultimoRegistroColeccionSig != null) {
                            long recorridosColeccionSig = ultimoRegistroColeccionSig.getRecorridos();
                            FindIterable<Document> resultado = mongoOperations
                                    .getCollection(coleccion_actual).find().skip((int) recorridosColeccionSig)
                                    .limit(5000);
                            List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                                    .collect(Collectors.toList());
                            Recolector recolector = new Recolector();
                            recolector.setColeccion_actual(coleccion_actual);
                            recolector.setColeccion_siguiente(coleccion_siguiente);
                            recolector.setTotal_datos(mongoOperations.getCollection(coleccion_actual).countDocuments());
                            recolector.setRecorridos(recorridosColeccionSig);
                            recolector.setFecha(new Date());
                            recolectorRepository.save(recolector);

                            List<Document> datosList = new ArrayList<>();
                            for (Document document : documentList) {
                                Document datos = (Document) document.get("datos");
                                datosList.add(datos);
                            }

                            return datosList;
                        } else {
                            FindIterable<Document> resultado = mongoOperations
                                    .getCollection(coleccion_actual).find().skip(0)
                                    .limit(5000);
                            List<Document> documentList = StreamSupport.stream(resultado.spliterator(), false)
                                    .collect(Collectors.toList());

                            Recolector recolector = new Recolector();
                            recolector.setColeccion_actual(coleccion_actual);
                            recolector.setColeccion_siguiente(coleccion_siguiente);
                            recolector.setTotal_datos(mongoOperations.getCollection(coleccion_actual).countDocuments());
                            recolector.setRecorridos(documentList.size());
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
                        recolector.setRecorridos(recorrido + documentList.size());
                        recolector.setFecha(new Date());
                        recolectorRepository.save(recolector);

                        List<Document> datosList = new ArrayList<>();
                        for (Document document : documentList) {
                            Document datos = (Document) document.get("datos");
                            datosList.add(datos);
                        }

                        return datosList;
                    }

                }

            } else {
                // no existe registros en la base de datos
                System.out.println("No hay registros, crear el primero");
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
                recolector.setRecorridos(documentList.size());
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

    public Recolector obtenerUltimoRegistroPorColeccion(String coleccionActual) {
        return recolectorRepository.obtenerUltimoRegistroPorColeccion(coleccionActual);
    }
}
