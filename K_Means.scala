package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
//import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.Dataset
import net.sansa_stack.rdf.spark.model.ds._

//import java com.hp.hpl.jena.rdf.arp.NTriples
import org.apache.jena.riot.Lang

object K_Means {
  def run(spark: SparkSession, featuredData_Df: DataFrame) {
    import spark.implicits._
    featuredData_Df.show(false)
    //featuredData_Df.select("Features").sample(true, .3).take(10).foreach(println)
    val x = featuredData_Df.select("Features")
    val x_rdd = x.rdd

   val xx = x_rdd.map(f=>{f.toString()})
   //xx.foreach(println)
  val t= xx.map(f=>{f.split("\\],").last.stripSuffix("])]").stripPrefix("[")})
  val data = t.map(f=>Vectors.dense(f.split(",").map(f=>f.toDouble))).cache()

  val len = data.map(f => f.size)
  print(len.max())
  val x1=len.max()
  //data.foreach(println)  
  
  val ssg = data.map(f => {
    var arr_temp = f.toArray
    arr_temp = arr_temp.padTo(x1, 0.0) 
    (Vectors.dense(arr_temp))
  })
  ssg.foreach(println)

val daataset = spark.read.format("libsvm").load("src/main/resources/kmeans_data.txt")
daataset.show(false)
  //val daataset = t.map(f=>f.split(",").map(f=>f.toDouble)).toDF()
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(daataset)
    val predictions = model.transform(daataset)
    predictions.show(false)
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")
    
    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)




/*
  val data2 = spark.sparkContext.textFile("src/main/resources/kmeans_data.txt")
  val parsedData = data2.map(s => Vectors.dense(s.split(' ').map(f=>f.toDouble))).cache()
  //parsedData.foreach(println)
  
  val numClusters = 2
  val numIterations = 20
  val clusters = KMeans.train(ssg, numClusters, numIterations)
  //println(clusters.clusterCenters)
  
  val WSSSE = clusters.computeCost(ssg)
  println(s"Within Set Sum of Squared Errors = $WSSSE")

  clusters.save(spark.sparkContext, "/home/pratik/Documents/KMeansExample/KMeansModel")
  val sameModel = KMeansModel.load(spark.sparkContext, "/home/pratik/Documents/KMeansExample/KMeansModel")
  println(sameModel.clusterCenters.length)
  
  import spark.implicits._
  val foo = sameModel.predict(ssg)
  foo.toDF("clusters").show
  */
  //val evaluator = new ClusteringEvaluator()
  //val silhouette = evaluator.evaluate(foo.toDF("features"))
  //println(s"Silhouette with squared euclidean distance = $silhouette")
    

  
  }
}