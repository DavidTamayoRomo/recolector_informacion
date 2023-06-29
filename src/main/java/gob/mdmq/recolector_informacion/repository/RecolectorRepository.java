package gob.mdmq.recolector_informacion.repository;


import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;

import gob.mdmq.recolector_informacion.model.Recolector;

public interface RecolectorRepository extends MongoRepository<Recolector, String> {
    
    @Aggregation(pipeline = {
        "{$match: {coleccion_actual: ?0}}",
        "{$sort: {_id: -1}}",
        "{$limit: 1}"
    })
    Recolector obtenerUltimoRegistroPorColeccion(String coleccion_actual);
    
}
