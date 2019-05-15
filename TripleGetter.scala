package net.sansa_stack.template.spark.rdf

package test.test

import java.io.BufferedReader

import java.io.BufferedWriter

import java.io.File

import java.io.FileNotFoundException

import java.io.FileOutputStream

import java.io.FileReader

import java.io.FileWriter

import java.io.IOException

import java.util.HashSet

import java.util.Iterator

import java.util.Set

import org.apache.jena.query.Query

import org.apache.jena.query.QueryExecution

import org.apache.jena.query.QueryExecutionFactory

import org.apache.jena.query.QueryFactory

import org.apache.jena.query.QuerySolution

import org.apache.jena.query.ResultSet

import org.apache.jena.query.ResultSetFormatter

import org.apache.jena.rdf.model.RDFNode

//remove if not needed
//import scala.collection.JavaConversions._
//import resource._ //use scala-arm from http://jsuereth.com/scala-arm/

object TripleGetter {

  def run() = {
    println("Inside Query: ")
//folder where predicates will be stored
    //val resultDir: File = new File("res/predicates/")
    //delteAllFiles(resultDir)
//for every single predicate - query all subject-object-pairs and list them in a file 'predicatename.txt'
    val results: ResultSet = sparqlGetPersons()
     //val file: File = new File("src/main/resources/triples.txt")
    //ResultSetFormatter.output(new FileWriter(file), results);
    //ResultSetFormatter.outputAsCSV(System.out, results)
    write(results)
  }
//file with listed predicates (create a hashset)
//File f = new File("res/test/predicates.txt");
//HashSet<String> predicates = getPredicates(f);
  /*String surname = "Meyer";
		String predicate = "http://dbpedia.org/property/title";
		ResultSet results = sparqlGetPersons(predicate, surname);
		write(results, predicate);*/

  /*for(String predicate : predicates) {
			String surname = "Meyer";
			ResultSet results = sparqlGetPersons();
			write(results);
		}*/

//file with listed predicates (create a hashset)
//File f = new File("res/test/predicates.txt");
//HashSet<String> predicates = getPredicates(f);
  /*String surname = "Meyer";
		String predicate = "http://dbpedia.org/property/title";
		ResultSet results = sparqlGetPersons(predicate, surname);
		write(results, predicate);*/

  /*for(String predicate : predicates) {
			String surname = "Meyer";
			ResultSet results = sparqlGetPersons();
			write(results);
		}*/

  def sparqlGetPersons(): ResultSet = {
//System.out.println("\nPredicate: "+predicate);
    val queryString: String = "PREFIX  rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" +
        "PREFIX  foaf: <http://xmlns.com/foaf/0.1/> \n" +
        "PREFIX  dbo: <http://dbpedia.org/ontology/> \n" +
        "select * where { \n" +
        "?subject a ?type . \n" +
        "VALUES ?type { dbo:TennisPlayer } . \n" +
        "?subject ?predicate ?object . \n" +
        "} \n" +
        ""
    val query: Query = QueryFactory.create(queryString)
    val qExe: QueryExecution =
      QueryExecutionFactory.sparqlService("https://dbpedia.org/sparql", query)
    val results: ResultSet = qExe.execSelect()
    println("end\n *************************************************\n")
//formatResults(results);
    return results
  }
//if(predicate.equals("http://dbpedia.org/property/title"))
//if(predicate.equals("http://dbpedia.org/property/title"))
/*
  def getPredicates(f: File): HashSet[String] = {
    val predicates: HashSet[String] = new HashSet[String]()
    for (br <- managed(new BufferedReader(new FileReader(f)))) {
      var line: String = null
      while ((line = br.readLine()) != null) {
        val predicate: String = line.split(",")(0)
        predicates.add(predicate)
      }
    }
    predicates
  }*/

  def write(results: ResultSet): Unit = {
//  File file = new File("res/predicates/"+pred+".txt");
    val file: File = new File("src/main/resources/triples_tennis.nt")
    val formattedResult: Set[String] = formatResults(results)
//ResultSetFormatter.outputAsTSV(fop, results);
    try {
      val out: BufferedWriter = new BufferedWriter(new FileWriter(file))
      val itr = formattedResult.iterator()
      while (itr.hasNext) {
        out.write(itr.next())
        out.newLine()
      }
      out.close()
    } catch {
      case e: IOException => e.printStackTrace()

    }
  }
//String pred = predicate.substring(predicate.lastIndexOf("/") + 1).trim();
// if(pred.contains("#")) pred = predicate.substring(predicate.lastIndexOf("#") + 1).trim();
//FileOutputStream fop = new FileOutputStream(file, true);
//String pred = predicate.substring(predicate.lastIndexOf("/") + 1).trim();
// if(pred.contains("#")) pred = predicate.substring(predicate.lastIndexOf("#") + 1).trim();
//FileOutputStream fop = new FileOutputStream(file, true);
import util.control.Breaks._

  private def formatResults(results: ResultSet): Set[String] = {
    val formattedResult: Set[String] = new HashSet[String]()
    while (results.hasNext) {
      val rowNo: Int = results.getRowNumber
      val qr: QuerySolution = results.next()
      val sub: RDFNode = qr.get("?subject")
      var subject: String = sub.toString
      //subject = subject.substring(subject.lastIndexOf("/") + 1).trim()
      val pred: RDFNode = qr.get("?predicate")
      var predicate: String = pred.toString
      //predicate = predicate.substring(predicate.lastIndexOf("/") + 1).trim()
      val obj: RDFNode = qr.get("?object")
      var `object`: String = obj.toString
      //if (obj.isURIResource)
        //`object` = `object`.substring(`object`.lastIndexOf("/") + 1).trim()
      if (obj.isLiteral)     
        if (`object`.contains("^")) {
          `object` = `object`.substring(0, `object`.indexOf("^")).trim()
           if(`object`.contains("\n")) {`object` = `object`.replaceAll("\n", " ")}
           formattedResult.add("<"+sub +"> <"+ pred + "> \""+  `object` +"\" ." ) }
        else if (`object`.contains("@en")) {
          //`object` = `object`.substring(0, `object`.indexOf("@")).trim()
           formattedResult.add("<"+sub +"> <"+ pred + "> \""+  `object`.substring(0, `object`.indexOf("@")).trim() +"\""+ `object`.substring(`object`.indexOf("@"), `object`.length()).trim()+" .")}
        else if (`object`.contains("@") && !`object`.contains("@en")) {
           formattedResult.add("<"+sub +"> <"+ pred + "> \""+  `object`.substring(0, `object`.indexOf("@")).trim() +"\""+ `object`.substring(`object`.indexOf("@"), `object`.length()).trim()+" .")}
        else {formattedResult.add("<"+sub +"> <"+ pred + "> \""+  obj +"\" ." )}
      if (obj.isURIResource)
        formattedResult.add("<"+sub +"> <"+ pred + "> <"+  `object` +"> ." )
    }
    formattedResult
  }

  def delteAllFiles(dir: File): Unit = {
    for (file <- dir.listFiles()) file.delete()
  }

}