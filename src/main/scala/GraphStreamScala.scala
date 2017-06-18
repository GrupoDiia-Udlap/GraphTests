import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.algorithm.{APSP, BetweennessCentrality, Eccentricity, PageRank}
import org.graphstream.algorithm.measure.ClosenessCentrality
import org.graphstream.graph.implementations.SingleGraph
import org.graphstream.graph.{Edge, Node}


object GraphStreamScala {
  case class interaccion(idinteraccion: String, atributos: Map[String, String]) //Interaccion datatype

  def main(args: Array[String]): Unit = {
    val configuration = new SparkConf()
      .setAppName("CassandraSparkTest").setMaster("local[*]")
      .set("spark.executor.memory", "1g")
      .set("spark.testing.memory", "2147480000")         // if you face any memory issues
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
    val sc = new SparkContext(configuration)

    //Read data from Cassandra
    import com.datastax.spark.connector._ //Loads implicit functions
    val interaccionesCassandra = sc.cassandraTable[interaccion]("diia", "interacciones").filter(
      interaccion => !(interaccion.atributos.get("nodo_destino").equals(""))
    )

    //Group by curso
    val grouped = interaccionesCassandra.groupBy(
      interaccion => interaccion.atributos.get("id_curso_origen")
    )

    //Calculate metrics by curso
    grouped.groupByKey().foreach(
      grupo => {
        //Build Graph
        val sg = new SingleGraph(grupo._1.toString)
        sg.setStrict(false)
        sg.setAutoCreate(true)
        grupo._2.foreach(
          interacciones => {  interacciones.foreach(
              interaccion => {
                //Add each interaccion
                sg.addEdge[Edge](interaccion.idinteraccion.toString, interaccion.atributos.get("nodo_origen").toString, interaccion.atributos.get("nodo_destino").toString)
              }
            )
          }
        )

        //Show node information
        System.out.println("Number of Nodes->" + sg.getNodeCount + "\n")
        import scala.collection.JavaConverters._
        sg.getNodeSet[Node]
          .asScala.toArray.foreach(
          (n: Node) => {
            System.out.println("nodeId->" + n.getId + "\n" + "inDegree->" + n.getInDegree + "\n" + "outDegree->" + n.getOutDegree + "\n")
          }
        )

        //Calculate Betweenness centrality
        val bcb = new BetweennessCentrality
        bcb.init(sg)
        bcb.compute()
        System.out.println("Betweenness:")
        sg.getNodeSet[Node]
          .asScala.toArray.foreach(
          (n: Node) => {
            System.out.println(n.getId + "->" + n.getAttribute("Cb"))
          }
        )

        //Calculate PageRank
        val pageRank = new PageRank
        pageRank.setVerbose(true) //Algorithm information
        pageRank.init(sg)
        pageRank.compute()
        System.out.println("Pagerank:")
        sg.getNodeSet[Node]
          .asScala.toArray.foreach(
          (n: Node) => {
            System.out.println(n.getId + "->" + n.getAttribute("PageRank"))
          }
        )

        //Before calculate the Eccentricity algorithm -> Calculate All Pair Shortest Path (APSP algorithm)
        val apsp = new APSP
        apsp.init(sg)
        apsp.setDirected(true)
        apsp.compute()

        val eccentricity = new Eccentricity
        eccentricity.init(sg)
        eccentricity.compute()
        System.out.println("Eccentricity:")
        sg.getNodeSet[Node]
          .asScala.toArray.foreach(
          (n: Node) => {
            System.out.println(n.getId + "->" + n.getAttribute("eccentricity"))
          }
        )

        val closeness: ClosenessCentrality = new ClosenessCentrality ()
        closeness.init(sg)
        closeness.compute()
        System.out.println("Closenness:")
        sg.getNodeSet[Node]
          .asScala.toArray.foreach(
          (n: Node) => {
            System.out.println(n.getId + "->" + n.getAttribute("closeness"))
          }
        )

      }
    )

  }
}