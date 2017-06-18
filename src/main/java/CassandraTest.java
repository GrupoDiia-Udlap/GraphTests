import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import models.Interaccion;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by Joseluis on 6/09/17.
 */
public class CassandraTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession//Spark configuration
                .builder()
                .master("local[*]")
                .appName("CassandraExample")
                //Cassandra configuration
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .config("spark.cassandra.auth.username", "cassandra")
                .config("spark.cassandra.auth.password", "cassandra")
                //.config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")// Set warehouse path
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());//Spark context


        // Prepare the schema
        CassandraConnector connector = CassandraConnector.apply(jsc.getConf());
        try (Session session = connector.openSession()) {
            //Create Keyspace
            session.execute("CREATE KEYSPACE IF NOT EXISTS diia WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
            //Create Table
            session.execute("CREATE TABLE IF NOT EXISTS diia.Interacciones ( idInteraccion text PRIMARY KEY, idOrigen text, tipoOrigen text, idDestino text, tipoDestino text, tipoInteraccion text, valorInteraccion text, idPrecedente text, idCurso text, fecha text, plataforma text );");
        }

        //Load CSV into DataFrame
        Dataset<Row> interaccionesDS = spark
                .read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header","true")
                .option("inferSchema", "true")
                .option("quote", "\"")
                //.schema(structType)
                .load("src/main/resources/group.csv");

        interaccionesDS.printSchema();//Show DataFrame schema
        interaccionesDS.show();//Show DataFrame data

        Encoder<Interaccion> interaccionEncoder = Encoders.bean(Interaccion.class);
        //Create RDD from DataFrame and change all column datatypes to string
        JavaRDD<Interaccion> interaccionesRDD = interaccionesDS
                .withColumn("idinteraccion", interaccionesDS.col("idinteraccion").cast("string"))
                .withColumn("idorigen", interaccionesDS.col("idorigen").cast("string"))
                .withColumn("tipoorigen", interaccionesDS.col("tipoorigen").cast("string"))
                .withColumn("iddestino", interaccionesDS.col("iddestino").cast("string"))
                .withColumn("tipodestino", interaccionesDS.col("tipodestino").cast("string"))
                .withColumn("tipointeraccion", interaccionesDS.col("tipointeraccion").cast("string"))
                .withColumn("valorinteraccion", interaccionesDS.col("valorinteraccion").cast("string"))
                .withColumn("idprecedente", interaccionesDS.col("idprecedente").cast("string"))
                .withColumn("idcurso", interaccionesDS.col("idcurso").cast("string"))
                .withColumn("fecha", interaccionesDS.col("fecha").cast("string"))
                .withColumn("plataforma", interaccionesDS.col("plataforma").cast("string")).as(interaccionEncoder).toJavaRDD();
        
        //Save data into Cassandra
        CassandraJavaUtil.javaFunctions(interaccionesRDD).writerBuilder("diia", "interacciones", CassandraJavaUtil.mapToRow(Interaccion.class)).saveToCassandra();
    }
}


