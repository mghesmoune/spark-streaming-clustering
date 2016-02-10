package org.lipn.clustering.batchStream
	 /**
	  * Copyright: LIPN, université Paris 13
	  * Projet : Square Predict
	  * << le code est confidentiel et aucun partage n'est autorisé...>> 
	  * */ 

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

class batchStream(
    var voisinage: Int, 
    var decayFactor: Double, 
    var lambdaAge : Double, 
    var nbNodesToAdd: Int, 
    var min_weight: Double   , 
    var max_age: Int, 
    var alphaErr: Double, 
    var d: Double) extends Serializable {
  
  def this() = this(voisinage=0, decayFactor=0.9, lambdaAge=1.2, nbNodesToAdd=3, min_weight=1, max_age=250, alphaErr=0.5, d=0.99)
  
  var model: batchStreamModel = new batchStreamModel()
      
  def getModel: batchStreamModel = model
  
  // Set the 'max inserted nodes' parameter. 
  def setMaxInsert(insert: Int): this.type = {
    this.nbNodesToAdd = insert
    this
  } 
  
  
  // Set the decay factor
  def setDecayFactor(alpha: Double): this.type = {
    this.decayFactor = alpha
    this
  }
  
  // Set the lambdaAge factor
  def setLambdaAge(lambda: Double): this.type = {
    this.lambdaAge = lambda
    this
  }
  // Set the 'min weight' of nodes parameter.
  def setMinWeight(w: Double): this.type = {
    this.min_weight = w
    this
  }
 
  // Set the 'max_age' parameter.
  def setMaxAge(a: Int): this.type = {
    this.max_age = a
    this
  }  
  
  // Set the alpha parameter.
  def setAlphaErr(a: Double): this.type = {
    this.alphaErr = a
    this
  } 
  
  // Set the d parameter.
  def setD(d: Double): this.type = {
    this.d = d
    this
  }   
  
  // Initializing the model.
  def initModelObj(txt: RDD[Array[Double]], dim: Int): batchStream = { 
    val nodes2 = txt.take(2)
    val node1 = nodes2(0)
    val node2 = nodes2(1)
    model.init2NodesObj(node1, node2, dim, 1)
    this 
  }  
 

  // Training on the model.
  def trainOnObj(data: DStream[pointObj], gstream: batchStream, dirSortie: String, dim: Int, nbWind: Int) {

    var timeUpdates = ArrayBuffer[Long](0L)
    var kk = 1
     data.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        val initialTimeUpdate = System.currentTimeMillis()
        println("\n<<<<<<<<<<<<<<<< >>>>>>>>>>>>>>>--batchStream--(batch: "+kk+" )..."+" rdd.count: "+rdd.count()+" \n")
        
        model = model.updateObj(rdd, gstream, kk, dim)
        timeUpdates += (timeUpdates(timeUpdates.size-1) + (System.currentTimeMillis() - initialTimeUpdate))
        if (timeUpdates.length > 100) timeUpdates.remove(0)
        
        if ((kk==1)|(kk==nbWind/9)|(kk==2*nbWind/9)|(kk==3*nbWind/9)|(kk==4*nbWind/9)|(kk==5*nbWind/9)|(kk==6*nbWind/9)|(kk==7*nbWind/9)|(kk==8*nbWind/9)|(kk>8*nbWind/9+10&kk%10==0)|(kk>=nbWind-2)){
          
          rdd.context.parallelize(model.toStringProto).saveAsTextFile(dirSortie+"/Prototypes-"+kk)
          rdd.context.parallelize(model.toStringOutdatedProto).saveAsTextFile(dirSortie+"/OutdatedProtos-"+kk)
          rdd.context.parallelize(model.edges).saveAsTextFile(dirSortie+"/Edges-"+kk)
          rdd.context.parallelize(model.clusterWeights).saveAsTextFile(dirSortie+"/Weights-"+kk)           
          rdd.context.parallelize(timeUpdates).saveAsTextFile(dirSortie+"/timeUpdates-"+kk)
          
          /*rdd.context.parallelize(model.toStringCard).saveAsTextFile(dirSortie+"/Cardinalities-"+kk)
          rdd.context.parallelize(model.toStringAss).saveAsTextFile(dirSortie+"/Assignments-"+kk)
          rdd.context.parallelize(model.toStringOutdatedAss).saveAsTextFile(dirSortie+"/AssignOutdatedNodes-"+kk)
          rdd.context.parallelize(model.toStringIdNode).saveAsTextFile(dirSortie+"/NodesIds-"+kk)
          rdd.context.parallelize(model.toStringIdOutdated).saveAsTextFile(dirSortie+"/OutdatedNodesIds-"+kk)
          
          rdd.context.parallelize(model.errors).saveAsTextFile(dirSortie+"/Errors-"+kk)
          rdd.context.parallelize(model.ages).saveAsTextFile(dirSortie+"/Ages-"+kk)
          rdd.context.parallelize(List(rdd.count())).saveAsTextFile(dirSortie+"/count-"+kk)
          */
          
        }
        kk+=1
      }
      else println("-- batchStream: empty rdd -- rdd.count : "+rdd.count())
    }    

    model
  }

}