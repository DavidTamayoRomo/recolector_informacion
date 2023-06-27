package gob.mdmq.recolector_informacion.model;

import java.util.Date;
import org.springframework.data.mongodb.core.mapping.Document;
import lombok.Data;


@Data
@Document(collection = "recolector")
public class Recolector {
    
    public String coleccion_actual;
    public String coleccion_siguiente;
    public String coleccion_anterior;
    public long total_datos;
    public long recorridos;
    public Date fecha;

}
