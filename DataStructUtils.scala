package com.zlemma.scala.utils

object DataStructUtils {
  import java.io._
  import scala.io._
  
	def xmlSpecialCharsReplace( str: String ) : String = {
    str replaceAll( "&", "&amp;")
  }
  
	def xmlAmpCharsReplace( str: String ) : String = {
    str replaceAll( "&amp;", "&")
	}
	
	def invertMap[A,B]( m: Map[A,B] ) : Map[B,List[A]] = {
	  m.groupBy{_._2}.mapValues{_.keys.toList}
	}
	
	def transposeMapOfLists[A,B]( mapOfLists: Map[A,List[B]] ) : List[Map[A,B]] = {
	  val k = ( mapOfLists keys ) toList
	  val l = ( k map { mapOfLists(_) } ) transpose;
	  l map {  v => ( k zip v ) toMap }
	}
	
	def transposeListOfMaps[A,B]( listOfMaps: List[Map[A,B]]) : Map[A,List[B]] = {
	  val k = ( listOfMaps(0) keys ) toList
	  val l = ( listOfMaps map { m => k map { m(_) } } ) transpose;
	  ( k zip l ) toMap
	}
	
	def transposeMapOfMaps[A,B,C]( mapOfMaps: Map[A,Map[B,C]] ) : Map[B,Map[A,C]] = {
	  val k = ( mapOfMaps keys ) toList
	  val listOfMaps = k map { mapOfMaps(_) }
	  val mapOfLists = transposeListOfMaps( listOfMaps )
	  mapOfLists mapValues { v => ( k zip v ) toMap }
	}
	
// Map("a" -> 1, "f" -> 3) map { x => x._2 }
	
	def irrTransposeListOfLists[A]( listOfLists : List[List[A]] ) : List[Map[Int,A]] = {
	  val lengths = listOfLists map { _.length }
    val indices = ( 0 to ( lengths.max - 1 ) ) toList;
    val indexZipMap = ( listOfLists zipWithIndex ) map { _.swap } toMap;
    indices map { i => indexZipMap filter { i < _._2.length } mapValues { _(i) } }
  }
	
	def irrTransposeMapOfLists[A,B]( mapOfLists : Map[A,List[B]] ) : List[Map[A,B]] = { 
	  val lengths = mapOfLists map { _._2.length }
    val indices = ( 0 to ( lengths.max - 1 ) ) toList;
    indices map { i => mapOfLists filter { i < _._2.length } mapValues { _(i) } }
  }
	
	def irrTransposeListOfMaps[A,B]( listOfMaps: List[Map[A,B]] ) : Map[A,Map[Int,B]] = {
	  val allKeys = ( listOfMaps map { m => ( m keys ) toList } ).flatten.distinct
	  val indices = ( 0 to ( listOfMaps.length - 1 ) ) toList
	  val res = ( allKeys map { k => ( k, indices filter { j => listOfMaps( j ).contains( k ) } map { j => ( j, listOfMaps( j )( k ) ) } toMap ) } ) toMap;
	  res
	}
	
	def irrTransposeMapOfMaps[A,B,C]( mapOfMaps: Map[A,Map[B,C]] ) : Map[B,Map[A,C]] = {
	  val innerKeys = ( mapOfMaps map { p => ( p._2.keys ) toList } ).toList.flatten.distinct
	  val outerKeys = ( mapOfMaps keys ) toList
	  val res = ( innerKeys map { ik => ( ik, outerKeys filter { ok => mapOfMaps( ok ).contains( ik ) } map { ok => ( ok, mapOfMaps( ok )( ik ) ) } toMap ) } ) toMap;
	  res
	}
	
	def listOfListsToMapOfMaps[A]( l : List[List[A]] ) : Map[Int,Map[Int,A]]= {
	  val listOfMaps = l map { x => ( x.zipWithIndex map {_.swap } ) toMap }
	  ( listOfMaps.zipWithIndex map { _.swap } ) toMap
	}
	
	def regularizeLists[A,B]( mapOfLists: Map[A,List[B]]) : Map[A,List[B]] = {
    val lengths = mapOfLists map { _._2.length }
    val minLength = lengths min;
    mapOfLists mapValues { _.take(minLength) }
	}
	
	def tableToCSV( table: List[Map[String,String]], csvSeparator: String) : String = {
	  val keys = ((table(0) keys) toList) sort { _ < _ }
	  val firstLine = keys mkString( "", csvSeparator, "")
	  val remLines = for ( row <- table ) yield {
	    val rowValues = keys map {row(_)}
	    rowValues mkString("", csvSeparator, "")
	  }
	  val allLines = firstLine :: remLines
	  allLines mkString( "", "\n", "")
	}
	
	def csvToTable( csv: String, sepRegExp: String  ) : List[Map[String,String]] = {
	  // example sepRegExp = """\$"""
	  val lines = csv.split("""\n""") toList
	  val firstLine = lines head;
	  val k = firstLine.split( sepRegExp )
	  for ( line <- lines tail ) yield {
	  	val v = line.split( sepRegExp )
	  	(k zip v) toMap
	  }
	}

	
	def readStringFromFile( fileName: String ) : String = {
	  val source = Source.fromFile( fileName )
	  val lines = source mkString;
	  source.close()
	  lines
	}
	
	def writeStringToFile( string: String, fileName: String ) : Unit = {
	  val fw = new FileWriter( fileName )
	  fw.write( string )
	  fw.close()
	}
	  
	def main( args: Array[String] ) : Unit = {
	  val m = Map( 903.5 -> Map( "x" -> 7, "y" -> 2, "z" -> 0 ), 32.3 -> Map( "x" -> 1, "y" -> 0 ), -17.3 -> Map( "y" -> 8, "x" -> 4, "a" -> 9 ) )
	  val mt = irrTransposeMapOfMaps( m )
	  val m1 = irrTransposeMapOfMaps( mt )
	  println( mt )
	  println( m )
	  println( m1 )
	  println( m == m1 )
	}
}