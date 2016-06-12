package com.zlemma.scala.genmetadata

import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.simpledb.AmazonSimpleDB
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest
import com.amazonaws.services.simpledb.model.CreateDomainRequest
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest
import com.amazonaws.services.simpledb.model.DeleteDomainRequest
import com.amazonaws.services.simpledb.model.Item
import com.amazonaws.services.simpledb.model.PutAttributesRequest
import com.amazonaws.services.simpledb.model.ReplaceableAttribute
import com.amazonaws.services.simpledb.model.ReplaceableItem
import com.amazonaws.services.simpledb.model.SelectRequest


import scala.collection.JavaConversions._

object SimpleDBUtils {

  def getSimpleDBHandle() : AmazonSimpleDB =
  	new AmazonSimpleDBClient( new PropertiesCredentials( new java.io.File(
  			"/Users/cover_drive/Documents/workspace/com.zlemma.scala/src/com/zlemma/scala/AwsCredentials.properties"
  	) ) )
  
  def getListOfSetMapsFromDomain( sdb : AmazonSimpleDB, domainName: String ) : List[Map[String,Set[String]]] = {
    try {
  		val selectRequest = new SelectRequest( "select * from `" + domainName + "`" )
  		val ab = for( item <- sdb.select( selectRequest ).getItems ) yield {
  			val row = for ( attribute <- item.getAttributes ) yield ( attribute.getName -> attribute.getValue )
  			row.groupBy( _._1 ).mapValues( x => ( ( x map { y => y._2 }) toSet ) )
  		}
  		ab.toList
  	} catch {
  	  case ase : AmazonServiceException => {
  	    println( "Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon SimpleDB, but was rejected with an error response for some reason.")
        println( "Error Message:    " + ase.getMessage )
        println( "HTTP Status Code: " + ase.getStatusCode )
        println( "AWS Error Code:   " + ase.getErrorCode )
        println( "Error Type:       " + ase.getErrorType )
        println( "Request ID:       " + ase.getRequestId )
        List()
  	  }
  	  case ace : AmazonClientException => {
  	  	println( "Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with SimpleDB, "
                    + "such as not being able to access the network.")
        println( "Error Message: " + ace.getMessage )
        List()
  	  }
  	  case ex => {
  	    println( ex )
  	    List()
  	  }
  	}
  }
  
  def convertToListOfStringMaps( lms: List[Map[String,Set[String]]] ) : List[Map[String,String]] = {
    for ( ms <- lms ) yield {
      ms mapValues { s =>
        if ( s.size == 1 ) 
          s.first
        else
          s.mkString( "[", ",", "]" )
      }
    }
  }
  
  def convertToListOfSetMaps( lms : List[Map[String,String]]) : List[Map[String,Set[String]]] = 
    lms map { x => ( x mapValues { y => Set(y) } ) }
  
  def createDomainFromListOfSetMaps( sdb : AmazonSimpleDB, domainName: String, lms : List[Map[String,Set[String]]] ) : Unit = {
    try {
    	sdb.createDomain( new CreateDomainRequest( domainName ) )
    	lms.zipWithIndex.foreach { x =>
    	  val id = "Item" + x._2
    	  val jlSize = x._1.foldLeft(0)( (acc,elem) => acc + elem._2.size )
    	  val atts = new java.util.ArrayList[ReplaceableAttribute]( jlSize )
    	  x._1.foreach { y => y._2.foreach { z => atts.add( new ReplaceableAttribute( y._1, z, true ) ) } }
    	  sdb.putAttributes( new PutAttributesRequest( domainName, id , atts ) )
    	}
  	} catch {
  	  case ase : AmazonServiceException => {
  	    println( "Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon SimpleDB, but was rejected with an error response for some reason.")
        println( "Error Message:    " + ase.getMessage )
        println( "HTTP Status Code: " + ase.getStatusCode )
        println( "AWS Error Code:   " + ase.getErrorCode )
        println( "Error Type:       " + ase.getErrorType )
        println( "Request ID:       " + ase.getRequestId )
  	  }
  	  case ace : AmazonClientException => {
  	  	println( "Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with SimpleDB, "
                    + "such as not being able to access the network.")
        println( "Error Message: " + ace.getMessage )
  	  }
  	  case ex => println( ex )
  	}
  }
  
  def createDomainFromListOfStringMaps( sdb : AmazonSimpleDB, domainName: String, lms : List[Map[String,String]] ) : Unit = {
    createDomainFromListOfSetMaps( sdb, domainName, convertToListOfSetMaps( lms ) )
  }
  
  def main(args: Array[String]): Unit = {
    val sdb = getSimpleDBHandle
//    val lofsms = getListOfSetMapsFromDomain( sdb, "TestDomain")
//    createDomainFromListOfSetMaps( sdb, "TestDomainClone", lofsms )
//    val lofsms1 = getListOfSetMapsFromDomain( sdb, "TestDomainClone" )
//    println( lofsms )
//    println( lofsms1 )
//    println( lofsms == lofsms1 )
    
    val lofsms = convertToListOfStringMaps( getListOfSetMapsFromDomain( sdb, "TestDomain") )
    createDomainFromListOfStringMaps( sdb, "TestDomainClone", lofsms )
    val lofsms1 = convertToListOfStringMaps( getListOfSetMapsFromDomain( sdb, "TestDomainClone" ) )
    println( lofsms )
    println( lofsms1 )
    println( lofsms == lofsms1 )
   
  }

}