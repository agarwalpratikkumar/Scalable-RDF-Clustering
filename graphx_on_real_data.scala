package net.sansa_stack.template.spark.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object graphx_on_real_data {
  
  def run(spark:SparkSession, both_entity_columns:RDD[(String, String)], dataset:Dataset[_], featuredData_Df:DataFrame) {
    println("In Graphx file:")
    //both_entity_columns.coalesce(1).foreach(println)
    val list_col_1 = both_entity_columns.map(f=>f._1).collect().toList
    val list_col_2 = both_entity_columns.map(f=>f._2).collect().toList
    val unique_entity_values = list_col_1.union(list_col_2).distinct    
    
    //val predicate_string_for_vertex = dataset.select("datasetA.attributes").rdd.map(f =>f.toString().replace("[", "").split(" ").toList(0)).top(unique_entity_values.length)
    
    val entities_for_vertex = spark.sparkContext.parallelize(unique_entity_values)
    //val predicate_string_for_vertex_ = spark.sparkContext.parallelize(predicate_string_for_vertex)
    //val  indexVertexID  = (predicate_string_for_vertex_ union entities_for_vertex).distinct().zipWithIndex()
    val  indexVertexID  = (entities_for_vertex).zipWithIndex()
    
    val  vertices:  RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))
    
    val tuples = both_entity_columns.keyBy(_._1).join(indexVertexID).map({
      case (k, ((s1,s2),l)) => (s2,(l,s1))
    })
    
    val  edges:  RDD[Edge[String]] = tuples.join(indexVertexID).map({
      case (k, ((si, p), oi)) =>  Edge(si, oi, p)
    })

    val  graph  =  Graph(vertices, edges)
    val ccGraph = graph.connectedComponents()
    
    val clusters = ccGraph.vertices.map(f => {
      val key = f._2.toString()
      val value = f._1.toString()
      (key,value)
    }).reduceByKey(_+','+_).map(f => (f._2))
    //clusters.foreach(println)
    println("Total Number of clusters are: ", clusters.count())
    
    prepare_input_for_silhoutte(spark,clusters, indexVertexID, featuredData_Df)
    
    
    println("Graphx file: exit")
    spark.stop
    
  }
  
  def prepare_input_for_silhoutte(spark:SparkSession, clusters: RDD[String], indexVertexID: RDD[(String, Long)], featuredData_Df:DataFrame) {
    val clusters_list = clusters.map(f => {
      f.split(",").toList
    }).map(_.map(_.toLong))
    val set_ = clusters_list.collect().toSet
    
    
    import spark.implicits._
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType}
    import org.apache.spark.sql.Row
    
    val schema = StructType(
        StructField("ids", LongType, true) ::
        StructField("prediction", IntegerType, true) :: Nil)
    var initialDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    
    var i = 0
    
    val all_id = indexVertexID.toDF("entity","ids").select("ids")
    
    set_.map(f => {
      val id_entity = f.toDF.intersect(all_id).toDF("ids")
      val clu = id_entity.withColumn("prediction", lit(i))
      i += 1
      initialDF = initialDF.union(clu)
    })
    
    val id_prediction_entity = initialDF.join(indexVertexID.toDF("entities","ids"),"ids")
    val silhoutte_input = id_prediction_entity.join(featuredData_Df,"entities").drop("attributes" , "words", "filtered_words")
    
    calculate_silhoutte(silhoutte_input)
  }
  
  def calculate_silhoutte(silhoutte_input: DataFrame) {
    println("calculating LSH Silhoutte:")
    val predictions = silhoutte_input.drop("entities")
    //predictions.show(false)
    
      import org.apache.spark.ml.evaluation.ClusteringEvaluator
      val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance for LSH = $silhouette")
    
    //calculate_kmeans_silhoutee(predictions)
  }
  
  def calculate_kmeans_silhoutee(prediction_data:DataFrame) {
    println("calculating kmeans Silhoutte: ")
    import org.apache.spark.ml.clustering.KMeans
    import org.apache.spark.ml.evaluation.ClusteringEvaluator
    
    val daataset = prediction_data.drop("prediction")
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(daataset)
    val predictions = model.transform(daataset)
    predictions.show(false)
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance for KMeans= $silhouette")
    
    //println("Cluster Centers: ")
    //model.clusterCenters.foreach(println)
  }
}