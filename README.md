

```scala211
//Import dependencies from Maven
classpath.add("org.apache.spark" % "spark-core_2.11" % "2.1.1")
classpath.add("org.apache.spark" % "spark-sql_2.11" % "2.1.1")
classpath.add("com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3")
classpath.add("org.graphstream" % "gs-core" % "1.3")
classpath.add("org.graphstream" % "gs-algo" % "1.3")

```

    Adding 114 artifact(s)
    Adding 13 artifact(s)
    Adding 4 artifact(s)
    Adding 5 artifact(s)
    Adding 7 artifact(s)



    



```scala211
//Import dependencies
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.algorithm.{APSP, BetweennessCentrality, Eccentricity, PageRank}
import org.graphstream.algorithm.measure.ClosenessCentrality
import org.graphstream.graph.implementations.SingleGraph
import org.graphstream.graph.{Edge, Node}

```


    [32mimport [36morg.apache.spark.{SparkConf, SparkContext}[0m
    [32mimport [36morg.graphstream.algorithm.{APSP, BetweennessCentrality, Eccentricity, PageRank}[0m
    [32mimport [36morg.graphstream.algorithm.measure.ClosenessCentrality[0m
    [32mimport [36morg.graphstream.graph.implementations.SingleGraph[0m
    [32mimport [36morg.graphstream.graph.{Edge, Node}[0m



```scala211
//Initialize Spark context
val configuration = new SparkConf()
      .setAppName("CassandraSparkTest").setMaster("local[*]")
      .set("spark.executor.memory", "1g")
      .set("spark.testing.memory", "2147480000")         // if you face any memory issues
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
val sc = new SparkContext(configuration)
```

    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    18/04/24 21:22:21 INFO SparkContext: Running Spark version 2.1.1
    18/04/24 21:22:21 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    18/04/24 21:22:22 WARN Utils: Your hostname, spark resolves to a loopback address: 127.0.1.1; using 192.168.80.129 instead (on interface eth0)
    18/04/24 21:22:22 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
    18/04/24 21:22:22 INFO SecurityManager: Changing view acls to: jose
    18/04/24 21:22:22 INFO SecurityManager: Changing modify acls to: jose
    18/04/24 21:22:22 INFO SecurityManager: Changing view acls groups to: 
    18/04/24 21:22:22 INFO SecurityManager: Changing modify acls groups to: 
    18/04/24 21:22:22 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(jose); groups with view permissions: Set(); users  with modify permissions: Set(jose); groups with modify permissions: Set()
    18/04/24 21:22:22 INFO Utils: Successfully started service 'sparkDriver' on port 36980.
    18/04/24 21:22:22 INFO SparkEnv: Registering MapOutputTracker
    18/04/24 21:22:22 INFO SparkEnv: Registering BlockManagerMaster
    18/04/24 21:22:22 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
    18/04/24 21:22:22 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
    18/04/24 21:22:22 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-b5766671-a87e-4aaf-ac0a-1718e2e01365
    18/04/24 21:22:23 INFO MemoryStore: MemoryStore started with capacity 1048.8 MB
    18/04/24 21:22:23 INFO SparkEnv: Registering OutputCommitCoordinator
    18/04/24 21:22:23 INFO Utils: Successfully started service 'SparkUI' on port 4040.
    18/04/24 21:22:23 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.80.129:4040
    18/04/24 21:22:23 INFO Executor: Starting executor ID driver on host localhost
    18/04/24 21:22:23 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 45332.
    18/04/24 21:22:23 INFO NettyBlockTransferService: Server created on 192.168.80.129:45332
    18/04/24 21:22:23 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
    18/04/24 21:22:23 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.80.129, 45332, None)
    18/04/24 21:22:23 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.80.129:45332 with 1048.8 MB RAM, BlockManagerId(driver, 192.168.80.129, 45332, None)
    18/04/24 21:22:23 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.80.129, 45332, None)
    18/04/24 21:22:23 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.80.129, 45332, None)



    [36mconfiguration[0m: [32mSparkConf[0m = org.apache.spark.SparkConf@1f459f6
    [36msc[0m: [32mSparkContext[0m = org.apache.spark.SparkContext@15c2ac5



```scala211
//Interaccion datatype
case class interaccion(idinteraccion: String, atributos: Map[String, String]) 
```


    defined [32mclass [36minteraccion[0m



```scala211
//Read interacciones from Cassandra
import com.datastax.spark.connector._ //Loads implicit functions
val interaccionesCassandra = sc.cassandraTable[interaccion]("diia", "interacciones").filter(
  interaccion => !(interaccion.atributos.get("nodo_destino").equals(""))
)
```


    [32mimport [36mcom.datastax.spark.connector._[0m
    [36minteraccionesCassandra[0m: [32morg[0m.[32mapache[0m.[32mspark[0m.[32mrdd[0m.[32mRDD[0m[[32minteraccion[0m] = MapPartitionsRDD[1] at filter at Main.scala:27



```scala211
//Group interacciones by id_curso
val grouped = interaccionesCassandra.groupBy(
  interaccion => interaccion.atributos.get("id_curso_origen")
)
```


    [36mgrouped[0m: [32morg[0m.[32mapache[0m.[32mspark[0m.[32mrdd[0m.[32mRDD[0m[([32mOption[0m[[32mString[0m], [32mIterable[0m[[32minteraccion[0m])] = ShuffledRDD[3] at groupBy at Main.scala:37



```scala211
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
```

    Number of Nodes->46
    
    nodeId->Some(24)
    inDegree->9
    outDegree->9
    
    nodeId->Some(3)
    inDegree->26
    outDegree->26
    
    nodeId->Some(19)
    inDegree->23
    outDegree->23
    
    nodeId->Some(17)
    inDegree->9
    outDegree->9
    
    nodeId->Some()
    inDegree->24
    outDegree->24
    
    nodeId->Some(13)
    inDegree->4
    outDegree->4
    
    nodeId->Some(29)
    inDegree->3
    outDegree->3
    
    nodeId->Some(7)
    inDegree->12
    outDegree->12
    
    nodeId->Some(14)
    inDegree->18
    outDegree->18
    
    nodeId->Some(18)
    inDegree->15
    outDegree->15
    
    nodeId->Some(4)
    inDegree->17
    outDegree->17
    
    nodeId->Some(10)
    inDegree->20
    outDegree->20
    
    nodeId->Some(5)
    inDegree->11
    outDegree->11
    
    nodeId->Some(25)
    inDegree->22
    outDegree->22
    
    nodeId->Some(16)
    inDegree->16
    outDegree->16
    
    nodeId->Some(20)
    inDegree->6
    outDegree->6
    
    nodeId->Some(21)
    inDegree->1
    outDegree->1
    
    nodeId->Some(0)
    inDegree->17
    outDegree->17
    
    nodeId->Some(22)
    inDegree->19
    outDegree->19
    
    nodeId->Some(1)
    inDegree->24
    outDegree->24
    
    nodeId->Some(9)
    inDegree->10
    outDegree->10
    
    nodeId->Some(23)
    inDegree->1
    outDegree->1
    
    nodeId->Some(11)
    inDegree->7
    outDegree->7
    
    nodeId->Some(34)
    inDegree->2
    outDegree->2
    
    nodeId->Some(46)
    inDegree->6
    outDegree->6
    
    nodeId->Some(26)
    inDegree->6
    outDegree->6
    
    nodeId->Some(2)
    inDegree->15
    outDegree->15
    
    nodeId->Some(32)
    inDegree->1
    outDegree->1
    
    nodeId->Some(15)
    inDegree->23
    outDegree->23
    
    nodeId->Some(33)
    inDegree->4
    outDegree->4
    
    nodeId->Some(27)
    inDegree->6
    outDegree->6
    
    nodeId->Some(44)
    inDegree->2
    outDegree->2
    
    nodeId->Some(37)
    inDegree->5
    outDegree->5
    
    nodeId->Some(30)
    inDegree->4
    outDegree->4
    
    nodeId->Some(50)
    inDegree->5
    outDegree->5
    
    nodeId->Some(6)
    inDegree->16
    outDegree->16
    
    nodeId->Some(35)
    inDegree->2
    outDegree->2
    
    nodeId->Some(28)
    inDegree->3
    outDegree->3
    
    nodeId->Some(42)
    inDegree->4
    outDegree->4
    
    nodeId->Some(38)
    inDegree->1
    outDegree->1
    
    nodeId->Some(45)
    inDegree->5
    outDegree->5
    
    nodeId->Some(36)
    inDegree->4
    outDegree->4
    
    nodeId->Some(43)
    inDegree->3
    outDegree->3
    
    nodeId->Some(40)
    inDegree->4
    outDegree->4
    
    nodeId->Some(31)
    inDegree->1
    outDegree->1
    
    nodeId->Some(12)
    inDegree->3
    outDegree->3
    
    Betweenness:
    Some(24)->10.923098198098197
    Some(3)->292.92687606682966
    Some(19)->180.91551693519187
    Some(17)->10.222871572871574
    Some()->268.38691374157315
    Some(13)->9.165416065416064
    Some(29)->3.074603174603175
    Some(7)->40.90775188863424
    Some(14)->62.98557932263815
    Some(18)->61.50316039309846
    Some(4)->66.13804713804714
    Some(10)->130.23951743303138
    Some(5)->21.780952380952378
    Some(25)->52.30443418409363
    Some(16)->89.75546838782131
    Some(20)->2.5333333333333337
    Some(21)->0.0
    Some(0)->130.53627069123974
    Some(22)->88.73595403850821
    Some(1)->170.7595280101162
    Some(9)->32.371008403361344
    Some(23)->0.0
    Some(11)->27.21428571428571
    Some(34)->0.4166666666666667
    Some(46)->0.7803030303030303
    Some(26)->0.4318181818181819
    Some(2)->121.67777313158119
    Some(32)->0.0
    Some(15)->135.83097366112844
    Some(33)->0.4444444444444444
    Some(27)->11.335714285714282
    Some(44)->0.0
    Some(37)->2.4611626287942077
    Some(30)->1.044047619047619
    Some(50)->0.7817460317460317
    Some(6)->27.098228146122885
    Some(35)->0.0
    Some(28)->1.2242424242424244
    Some(42)->0.3246753246753247

         1

    
    Some(38)->0.0
    Some(45)->0.36764705882352944
    Some(36)->0.40404040404040403
    Some(43)->0.0
    Some(40)->1.9959298871063575
    Some(31)->0.0
    Some(12)->0.0
    Pagerank:
    Some(24)->0.020194221567667453
    Some(3)->0.05701234186301448
    Some(19)->0.04857787683180915
    Some(17)->0.01992092096697594
    Some()->0.05340180074443037
    Some(13)->0.011858830457909543
    Some(29)->0.009347025699505315
    Some(7)->0.026709681982389613
    Some(14)->0.03739590730995127
    Some(18)->0.032479992006803765
    Some(4)->0.03607258734342611
    Some(10)->0.04221049578619158
    Some(5)->0.02442016530616005
    Some(25)->0.04401428315590487
    Some(16)->0.03425249629661635
    Some(20)->0.014291814292965367
    Some(21)->0.00515217008566886
    Some(0)->0.03775722689748393
    Some(22)->0.03984889025462498
    Some(1)->0.0497634736368291
    Some(9)->0.022868301408416846
    Some(23)->0.00515217008566886
    Some(11)->0.017229552116489816
    Some(34)->0.007172680329564316
    Some(46)->0.01406948760145634
    Some(26)->0.014022550125168485
    Some(2)->0.034053046843134434
    Some(32)->0.005190538423481358
    Some(15)->0.047308331265106

          0.73235749
         2      0.20579352
         3      0.05010714
         4      0.01614693
         5      0.00464566
         6      0.00172797
         7      0.00058368
         8      0.00022593
         9      0.00008341
        10      0.00003283
        11      0.00001287
        12      0.00000512


    814
    Some(33)->0.010715146573543456
    Some(27)->0.015026298051122756
    Some(44)->0.0069467542863064224
    Some(37)->0.012547254391071983
    Some(30)->0.010465922998288
    Some(50)->0.012260203225733332
    Some(6)->0.032740967439106726
    Some(35)->0.006920017942473922
    Some(28)->0.00895559216529451
    Some(42)->0.010436457600536379
    Some(38)->0.00514874612227441
    Some(45)->0.012497648051343842
    Some(36)->0.010562881990881299
    Some(43)->0.008537584275928052
    Some(40)->0.010732438397376719
    Some(31)->0.005124746302794318
    Some(12)->0.008632479501108691
    Eccentricity:
    Some(24)->false
    Some(3)->false
    Some(19)->true
    Some(17)->false
    Some()->true
    Some(13)->false
    Some(29)->false
    Some(7)->false
    Some(14)->true
    Some(18)->false
    Some(4)->false
    Some(10)->true
    Some(5)->false
    Some(25)->true
    Some(16)->false
    Some(20)->false
    Some(21)->false
    Some(0)->true
    Some(22)->false
    Some(1)->true
    Some(9)->false
    Some(23)->false
    Some(11)->false
    Some(34)->false
    Some(46)->false
    Some(26)->false
    Some(2)->false
    Some(32)->false
    Some(15)->true
    Some(33)->false
    Some(27)->false
    Some(44)->false
    Some(37)->false
    Some(30)->false
    Some(50)->false
    Some(6)->false
    Some(35)->false
    Some(28)->false
    Some(42)->false
    Some(38)->false
    Some(45)->false
    Some(36)->false
    Some(43)->false
    Some(40)->false
    Some(31)->false
    Some(12)->false
    Closenness:
    Some(24)->0.011764705882352941
    Some(3)->0.015151515151515152
    Some(19)->0.014925373134328358
    Some(17)->0.011494252873563218
    Some()->0.015151515151515152
    Some(13)->0.009708737864077669
    Some(29)->0.009708737864077669
    Some(7)->0.012048192771084338
    Some(14)->0.0136986301369863
    Some(18)->0.01282051282051282
    Some(4)->0.012987012987012988
    Some(10)->0.014084507042253521
    Some(5)->0.011904761904761904
    Some(25)->0.014492753623188406
    Some(16)->0.013333333333333334
    Some(20)->0.010869565217391304
    Some(21)->0.00909090909090909
    Some(0)->0.0136986301369863
    Some(22)->0.013888888888888888
    Some(1)->0.014925373134328358
    Some(9)->0.011627906976744186
    Some(23)->0.00909090909090909
    Some(11)->0.011235955056179775
    Some(34)->0.008695652173913044
    Some(46)->0.011111111111111112
    Some(26)->0.01098901098901099
    Some(2)->0.013157894736842105
    Some(32)->0.008333333333333333
    Some(15)->0.014705882352941176
    Some(33)->0.010416666666666666
    Some(27)->0.010526315789473684
    Some(44)->0.00909090909090909
    Some(37)->0.010638297872340425
    Some(30)->0.010101010101010102
    Some(50)->0.010638297872340425
    Some(6)->0.013333333333333334
    Some(35)->0.01
    Some(28)->0.00980392156862745
    Some(42)->0.010416666666666666
    Some(38)->0.008547008547008548
    Some(45)->0.010526315789473684
    Some(36)->0.010309278350515464
    Some(43)->0.009615384615384616
    Some(40)->0.01020408163265306
    Some(31)->0.00909090909090909
    Some(12)->0.00980392156862745



    

