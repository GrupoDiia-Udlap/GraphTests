package models;

/**
 * Created by joseluis on 6/09/17.
 */
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.LocalDate;
import org.apache.spark.sql.Dataset;

public class Interaccion implements Serializable{
    private String idinteraccion;
    private Map<String, String> atributos;
    public Interaccion(){

    }

    public Interaccion(String idinteraccion, Map<String, String> atributos) {
        this.idinteraccion = idinteraccion;
        this.atributos = atributos;
    }

    public String getidinteraccion() {
        return idinteraccion;
    }

    public void setidinteraccion(String idinteraccion) {
        this.idinteraccion = idinteraccion;
    }

    public Map<String, String> getatributos() {
        return atributos;
    }

    public void setatributos(Map<String, String> atributos) {
        this.atributos = atributos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Interaccion that = (Interaccion) o;

        return idinteraccion != null ? idinteraccion.equals(that.idinteraccion) : that.idinteraccion == null;
    }

    @Override
    public int hashCode() {
        return idinteraccion != null ? idinteraccion.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Interaccion{" +
                "idinteraccion='" + idinteraccion + '\'' +
                '}';
    }
}
