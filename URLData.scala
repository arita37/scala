package com.zlemma.scala.utils

import java.net.{URLConnection, URL}
import scala.io.Source

class URLData( val urlStr : String ) {
	def getText : String = {	  
		var url = new URL( urlStr )
    val connect = url.openConnection
    val stream = connect.getInputStream
    Source.fromInputStream( stream ).mkString("") 
	}
	
	def saveTextToFile( fileName: String ) : Unit = DataStructUtils.writeStringToFile( getText, fileName )
}

object URLData {
  def main( args : Array[String] ) : Unit = {
    val obj = new URLData( "http://www.maxmind.com/GeoIPCity-534-Location.csv" )
    val data = obj.getText
    println( data )
//    obj.saveTextToFile( "/Users/cover_drive/Documents/Geo/MaxMind/GeoIPCity.csv" )
  }
}