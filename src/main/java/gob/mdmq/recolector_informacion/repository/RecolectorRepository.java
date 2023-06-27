package gob.mdmq.recolector_informacion.repository;


import org.springframework.data.mongodb.repository.MongoRepository;

import gob.mdmq.recolector_informacion.model.Recolector;

public interface RecolectorRepository extends MongoRepository<Recolector, String> {
    

    
}
