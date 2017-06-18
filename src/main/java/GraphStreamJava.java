import com.datastax.spark.connector.japi.CassandraJavaUtil;
import models.Interaccion;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.graphstream.algorithm.APSP;
import org.graphstream.algorithm.BetweennessCentrality;
import org.graphstream.algorithm.Eccentricity;
import org.graphstream.algorithm.PageRank;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * Created by joseluis on 6/09/17.
 */
public class GraphStreamJava {
    public static void main(String[] args) {
        //Spark configuration
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("CassandraExample")
                //Cassandra configuration
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .config("spark.cassandra.auth.username", "cassandra")
                .config("spark.cassandra.auth.password", "cassandra")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());//Spark context

        //Read data from Cassandra
        JavaRDD<Interaccion> interaccionesCassandra = javaFunctions(jsc).cassandraTable("diia", "interacciones", CassandraJavaUtil.mapRowTo(Interaccion.class))
                //Only interactions with nodo_destino
                .filter(
                        interaccion -> !interaccion.getatributos().get("nodo_destino").equals("")
                );

        //Group by curso
        JavaPairRDD<String, Iterable<Interaccion>> grouped = interaccionesCassandra.groupBy(
                interaccion -> interaccion.getatributos().get("id_curso_origen")
        );

        //Calculate metrics by curso
        grouped.groupByKey().foreach(
                grupo -> {
                    //Build Graph
                    SingleGraph sg = new SingleGraph(grupo._1());
                    sg.setStrict(false);
                    sg.setAutoCreate(true);
                    grupo._2().forEach(
                            interacciones -> interacciones.forEach(
                                    interaccion-> {
                                        //Add each interaccion
                                        sg.addEdge(interaccion.getidinteraccion(), interaccion.getatributos().get("nodo_origen"), interaccion.getatributos().get("nodo_destino"));
                                    }
                            )
                    );

                    //Show node information
                    System.out.println("Number of Nodes->"+sg.getNodeCount()+"\n");
                    for(Node n:sg.getEachNode()) {
                        System.out.println("nodeId->" + n.getId() + "\n" +
                                           "inDegree->" + n.getInDegree() + "\n" +
                                           "outDegree->" + n.getOutDegree() + "\n");
                    }

                    //Calculate Betweenness centrality
                    BetweennessCentrality bcb = new BetweennessCentrality();
                    bcb.init(sg);
                    bcb.compute();
                    System.out.println("Betweenness:");
                    for(Node n:sg.getEachNode()) {
                        System.out.println(n.getId()+"->" + n.getAttribute("Cb"));
                    }

                    //Calculate PageRank
                    PageRank pageRank = new PageRank();
                    pageRank.setVerbose(true);//Algorithm information
                    pageRank.init(sg);
                    pageRank.compute();
                    System.out.println("Pagerank:");
                    for(Node n:sg.getEachNode()) {
                        System.out.println(n.getId()+"->"+n.getAttribute("PageRank"));
                    }

                    //Before calculate the Eccentricity algorithm -> Calculate All Pair Shortest Path (APSP algorithm)
                    APSP apsp = new APSP();
                    apsp.init(sg);
                    apsp.setDirected(true);
                    apsp.compute();

                    Eccentricity eccentricity = new Eccentricity();
                    eccentricity.init(sg);
                    eccentricity.compute();

                    System.out.println("Eccentricity:");
                    for(Node n:sg.getEachNode()) {
                        System.out.println(n.getId()+"->" + n.getAttribute("eccentricity"));
                    }

                    System.out.println("Closenness:");
                    for(Node x:sg.getEachNode()){
                        APSP.APSPInfo info;
                        Double sum = 0.0;
                        for(Node y:sg.getEachNode()){
                            if(!x.getId().equals(y.getId())){
                                info = x.getAttribute(APSP.APSPInfo.ATTRIBUTE_NAME);
                                sum += info.getShortestPathTo(y.getId()).getEdgeCount();
                            }
                        }
                        Double closenness = (1 / sum) * (sg.getNodeCount()-1);
                        System.out.println(x.getId()+"->" + closenness);
                    }
                }
        );


    }
}
