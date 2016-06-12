package com.zlemma.scala.utils

import java.io._
import scala.io.Source
import java.net.URL
import scala.xml._
import java.nio.channels.Channels

class HTMLAsXML( val url: String ) {
	
	def getXML : Node = {  
	  // println( "Getting XML from " + url )
		val parserFactory = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
		val parser = parserFactory.newSAXParser()
		val source = new org.xml.sax.InputSource( url )
		val adapter = new scala.xml.parsing.NoBindingFactoryAdapter
		HTMLAsXML.adapter.loadXML(source, HTMLAsXML.parser)
	}
	
	def getXMLString: String = {
	  getXML.toString
	}
	
	def getXMLForTag( tag: String) : Node = {
	  val fullStr = scala.io.Source.fromInputStream(new URL(url).openConnection.getInputStream).getLines.mkString("")
	  val i1 = fullStr.indexOf("<" + tag)
	  val i2 = fullStr.lastIndexOf("</" + tag + ">") + tag.size + 3
	  val subStr = fullStr.substring(i1, i2)
		val source = new InputSource(new StringReader(subStr))
		(HTMLAsXML.adapter.loadXML(source, HTMLAsXML.parser) \\ tag)(0)
	}
	
	def saveToFile( fileName: String ) : Unit = HTMLAsXML.saveXMLToFile( getXML, fileName )
}
	
	
object HTMLAsXML {
  
  def getXMLFromURL( url: String ) = new HTMLAsXML( url ) getXML
  
  val parser = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl().newSAXParser
  
  val adapter = new scala.xml.parsing.NoBindingFactoryAdapter
  
  def saveXMLToFile( node: Node, fileName: String ) : Unit = {
    val Encoding = "UTF-8"
    val pp = new PrettyPrinter(80, 2)
    val fos = new FileOutputStream( fileName )
    val writer = Channels.newWriter(fos.getChannel(), Encoding)
    try {
      writer.write("<?xml version='1.0' encoding='" + Encoding + "'?>\n")
      writer.write(pp.format(node))
    } finally {
      writer.close()
    }
    fileName
  }

}

