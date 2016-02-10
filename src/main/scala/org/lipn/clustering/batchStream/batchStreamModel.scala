package org.lipn.clustering.batchStream
	 /**
	  * Copyright: please refer to the README.md file
	  * User: ghesmoune
	  * Date: 01/01/2016
	  * Project : Square Predict (http://square-predict.net/)
	  * */ 
import scala.collection.mutable.ArrayBuffer
import breeze.linalg.{Vector, squaredDistance}
import org.apache.spark.rdd.RDD
import scala.math.{abs, exp}
  
class batchStreamModel(
    var nodes: ArrayBuffer[prototype], 
    var outdatedNodes: ArrayBuffer[prototype], 
    var isolatedNodes: ArrayBuffer[prototype], 
    var edges: ArrayBuffer[ArrayBuffer[Int]], 
    var ages: ArrayBuffer[ArrayBuffer[Double]], 
    var errors: ArrayBuffer[Double], 
    var clusterWeights: ArrayBuffer[Double] 
    ) extends Serializable {
   
  
  def this() = this(
      nodes = ArrayBuffer(),
      outdatedNodes = ArrayBuffer(),
      isolatedNodes = ArrayBuffer(),
      edges = ArrayBuffer(),
      ages = ArrayBuffer(),
      errors = ArrayBuffer(),
      clusterWeights = ArrayBuffer()
      )
  
  // initialize the model with 2 NodesObj    
  def init2NodesObj(n1: Array[Double], n2: Array[Double], dim: Int, idPoint: Int): batchStreamModel = {
    this.nodes.+=(this.pointToProto(n1, dim, idPoint))
    this.nodes.+=(this.pointToProto(n2, dim, idPoint+1))
    this.edges = ArrayBuffer(ArrayBuffer(0,1),ArrayBuffer(1,0))
    this.ages = ArrayBuffer(ArrayBuffer(Double.NaN,0),ArrayBuffer(0,Double.NaN))
    this.errors = ArrayBuffer(0,0)
    this.clusterWeights = ArrayBuffer(1, 1)
    this
  }
  
  // point To Object
	def pointToObjet(e: Array[Double], dim: Int, labId: Int) = {
    val dataPart = e.take(e.size - labId) //labId=-2 because the 2 last columns represent labels & id
    val part1 = Vector(dataPart.take(dim))
    val etiq = e.drop(dim).map(_.toInt)
    new pointObj(part1, etiq(0), etiq(1))
  } 
   
	// pointToProto
	def pointToProto(e: Array[Double], dim: Int, idPoint: Int) = {
    val dataPart = e.take(e.size - 2) //-2 because the 2 last columns represent labels & id
    val part1 = Vector(dataPart.take(dim))
    new prototype(part1, Set(idPoint), this.nodes.length+1) 
  } 	
	
	// updateObj : the main update method
  def updateObj(rdd: RDD[pointObj], gstream: batchStream, kk: Int, dim: Int): batchStreamModel = {
    // the mapping step     
    val closestNum = rdd.map(point => this.findTwoNearestPointDist1L(this.nodes, point)) 
       
    // get sums and counts for updating each cluster
		val mergeContribs: ((Array[Int], Double, Vector[Double], Long, Set[Int]), (Array[Int], Double, Vector[Double], Long, Set[Int])) =>
		  (Array[Int], Double, Vector[Double], Long, Set[Int]) = (p1, p2) => {
		    val y_p11 = addPairwiseInt(p2._1, p1._1)
		    (y_p11, p1._2 + p2._2, p2._3 + p1._3, p1._4 + p2._4, p1._5 union p2._5)
		  }
	
		val dim = this.nodes(0).protoPartNum.size
		val nbNodes = this.nodes.size
		
		// the reduce step 
	  val pointStats: Array[(Int,(Array[Int], Double, Vector[Double], Long, Set[Int]))] = closestNum
	                        .aggregateByKey((Array.fill(nbNodes)(0), 0.0, Vector.zeros[Double](dim), 0L, Set[Int]()))(mergeContribs, mergeContribs)
	                        .collect()
		
    // implement update rule 
		updateRule(pointStats, kk, dim, gstream.decayFactor, gstream.lambdaAge, gstream.voisinage)
		
		// remove edges having an age > max_age 
    removeOldEdges(gstream.max_age)
    
		// delete each isolated node
    removeIsolatedNodes()
    
		// update the global error
		upGlobalErrors(pointStats)
	  
		// applying the fading function
		if (nbNodes > 100 & kk % 3 == 0)
      fading(gstream.min_weight)
		
		// delete each isolated node
    removeIsolatedNodes()    
		
		// add new nodes --x2 or x3
    if (nbNodes <= 300 & kk % 5 == 0)
      addNewNodes(gstream.nbNodesToAdd, kk, dim, gstream.alphaErr)
		
		// decrease the error of all units.
    for (i <- 0 to this.errors.size-1) this.errors(i) *= gstream.d
      
  this  
  }

  
  // find the two nearest nodes ti the input data-point
  def findTwoNearestPointDist1L(nodes: ArrayBuffer[prototype], input: pointObj) = {
    var nbNodes = nodes.length 
    var distances: ArrayBuffer[Double] = ArrayBuffer()
    for (i<-0 to nbNodes-1){
      distances.+=(squaredDistance(input.pointPartNum, nodes(i).protoPartNum)) 
    }
    val (sdistances, indices) = distances.zipWithIndex.sorted.unzip
    val clos1 = indices(0)
    val clos2 = indices(1)
    
    var clos2VecBinaire = Array.fill(nbNodes)(0)  
    clos2VecBinaire(clos2) = 1
        
    (clos1, (clos2VecBinaire, sdistances(0), input.pointPartNum, 1L, Set(input.id))) 
  }
 

  // euclideanDistance
  def euclideanDistance(a: Array[Double], b: Array[Double]): Double = {
    val values = (a zip b).map{ case (x, y) => x - y }
    val size = values.size
    var sum = 0.0
    var i = 0
    while (i < size) {
      sum += values(i) * values(i)
      i += 1
    }
    math.sqrt(sum)
  }  
 
  // implement update rule 
  def updateRule(pointStats: Array[(Int, (Array[Int], Double, Vector[Double], Long, Set[Int]))], 
      kk: Int, dim: Int, decayFactor: Double, lambdaAge: Double, voisinage: Int) = {
    val discount  = decayFactor	  
	  // apply discount to weights
		this.clusterWeights = this.scal(discount, this.clusterWeights)
		
		pointStats.foreach { case (label, (bmu2, errs, sum, count, idsData)) =>
		  
		  // increment the age of all edges emanating from s1.
		  val s1_Neighbors = this.edges(label).zipWithIndex.filter(_._1 == 1).map(_._2)
		  val SizeOfNeighborhood = s1_Neighbors.size
		  // ages(s1_Neighbors,s1) = ages(s1_Neighbors,s1)+age_inc;
		  for(i <- 0 to SizeOfNeighborhood-1){
		    this.ages(s1_Neighbors(i))(label) *= lambdaAge
		    this.ages(s1_Neighbors(i))(label) += 1
		    this.ages(label)(s1_Neighbors(i)) = this.ages(s1_Neighbors(i))(label)
		    //this.ages(label)(s1_Neighbors(i)) *= lambdaAge
		    //this.ages(label)(s1_Neighbors(i)) += 1
		  }

		  // merge old ids with the ids of the last window 
		  this.nodes(label).idsDataAssigned = this.nodes(label).idsDataAssigned union idsData
		  
		  val v1 = scal(this.clusterWeights(label), this.nodes(label).protoPartNum)
		  var nominateur = v1 + sum
		  var denominateur = this.clusterWeights(label) + count

		  if (voisinage == 1) {
			  var Tsumi :Vector[Double] = Vector.zeros[Double](dim) 		    
			  var Tcounti = 0.0
			  val label_Neighbors = this.edges(label).zipWithIndex.filter(_._1 == 1).map(_._2)
			  label_Neighbors.foreach{ e =>
				  val tmp = pointStats.filter(p => p._1 == e)
				  if (tmp.length > 0) {
					  val counti = tmp.map( _._2._4).reduceLeft[Long](_+_)
					  val sumi = tmp.map( _._2._3).reduceLeft[Vector[Double]](_+_)

					  val k = this.kNeighbor(this.nodes(label), this.nodes(e), this.temperature)
					  Tsumi += k * sumi 					  
					  Tcounti += k * counti
				  }
			  }
			  nominateur += Tsumi
			  denominateur += Tcounti
		  }

		  val ctplus1 = scal(1 / math.max(denominateur, 1e-16), nominateur)
		    
      // move each centroid		  
      this.clusterWeights(label) = this.clusterWeights(label) + count
 		  this.nodes(label).protoPartNum = ctplus1
 
		  
		  val idxMaxValue = bmu2.zipWithIndex.maxBy(_._1)._2
			  
		  // create an edge (label,idxMaxValue)
      // if s1 and s2 are connected by an edge, set the age of this edge to zero. If such an edge does not exist, create it.
      this.edges(label)(idxMaxValue) = 1;  
		  this.edges(idxMaxValue)(label) = 1
      this.ages(label)(idxMaxValue) = 0;  
		  this.ages(idxMaxValue)(label) = 0
      
      // update the error variable
      this.errors(label) += errs
		  
		} //end update-rule
  }

  // remove old edges   
  def removeOldEdges(maxAge: Int) = {
    var DelRowCol: ArrayBuffer[ArrayBuffer[Int]] = ArrayBuffer()
    	for (i<-0 to this.ages.size-1) {
      	DelRowCol += this.ages(i).zipWithIndex.filter(_._1 > maxAge).map(_._2) 
    	}
    	val sizeDelRowCol = DelRowCol.size 
    	var DelCol: ArrayBuffer[Int] = ArrayBuffer()
    	for (i<-0 to sizeDelRowCol-1) {
      	DelCol = DelRowCol(i)
      	DelCol.foreach { x =>
        	this.edges(i)(x) = 0 
        	this.ages(i)(x) = Double.NaN 
      	}
    	}
  }  

	// delete each isolated node
  def removeIsolatedNodes() = {
    val nbrNodes = this.nodes.size
    var nbNodesAct = this.edges.head.size 
    if (nbrNodes != nbNodesAct) {
    	throw new IllegalStateException("The size of nodes and edges must be equal, edges must be a square matrix")
    }

		for (j<-nbrNodes-1 to 0 by -1) {
			if (this.edges(j).equals(ArrayBuffer.fill(nbNodesAct)(0))) { 
			  
			  // save this isolated node
			  this.isolatedNodes += this.nodes(j)
			  
			  // delete
				this.edges = this.removeLineColInt(j, this.edges)
				this.ages = this.removeLineCol(j, this.ages)
				this.nodes.remove(j)
				this.clusterWeights.remove(j)
				this.errors.remove(j)

				nbNodesAct -= 1
			}
		} 
  }

  // update the global errors
  def upGlobalErrors(pointStats: Array[(Int, (Array[Int], Double, Vector[Double], Long, Set[Int]))]) = {
		val erreurs = pointStats.groupBy(_._1).mapValues(_.map(_._2._2).sum).toArray //erreurs: (label, error)
		for (er <- erreurs) {
		  if (this.errors.size < er._1)
		    this.errors(er._1) += er._2
		}    
  }

  // add new nodes --x3
  def addNewNodes(nbNodesToAdd: Int, kk: Int, dim: Int, alphaErr: Double) = {
		for (j <- 1 to nbNodesToAdd) {
		  // find the node with the largest error		
		  val q = this.errors.indexOf(this.errors.max)
		  
		  //val (errs, idxErr) = erreurs.zipWithIndex.sorted.unzip
		  val q_Neighbors = this.edges(q).zipWithIndex.filter(_._1 == 1).map(_._2)
      
		  // find the neighbor f with the largest accumulated error 
      val f = this.errors.indexOf(this.errors.zipWithIndex.collect{case(a,b) if q_Neighbors.exists(_==b) => a}.max)

      // Add the new node half-way between nodes q and f: nodes = [nodes .5*(nodes(:,q)+nodes(:,f))]
      val node1 = this.nodes(q).protoPartNum + this.nodes(f).protoPartNum
      node1 :*= 0.5 
      val protoNode1 = new prototype(node1, /*Vector.zeros(dim - sizeNumPart),*/ Set(), this.nodes.length+1) 
      this.nodes.+=(protoNode1)		

 		  this.clusterWeights.+=(0)
 		  
      // Remove the original edge between q and f.
      this.edges(q)(f) = 0;  
		  this.edges(f)(q) = 0    
      this.ages(q)(f) = Double.NaN;  
		  this.ages(f)(q) = Double.NaN    
      // insert edges connecting the new unit r with units q and f.
      val r = this.edges.size
      this.edges = this.addElementLast(0, this.edges)
      this.edges(q)(r) = 1;  
		  this.edges(r)(q) = 1
      this.edges(f)(r) = 1;  
		  this.edges(r)(f) = 1
    
    	this.ages = this.addElementLast(Double.NaN, this.ages)
    	this.ages(q)(r) = 0; 
		  this.ages(r)(q) = 0
    	this.ages(f)(r) = 0; 
		  this.ages(r)(f) = 0
    
      this errors(q) = this.errors(q) * alphaErr
      this errors(f) = this.errors(f) * alphaErr 
      this.errors.+= ((this.errors(q) + this.errors(f)))
        
		}    
  }
  
  // fading function
  def fading(minWeight: Double) = {
    if (nodes.size != clusterWeights.size) {
    	throw new IllegalStateException("The size of nodes and weights must be equal !")
    }

    val weightsWithIndex = this.clusterWeights.view.zipWithIndex
    val (minW, smallest) = weightsWithIndex.minBy(_._1)
    if (minW < minWeight) {
      // save this node as an outdated node
      this.outdatedNodes += this.nodes(smallest)
      
      // delete
      this.edges = this.removeLineColInt(smallest, this.edges)
      this.ages = this.removeLineCol(smallest, this.ages)        
      this.clusterWeights.remove(smallest)
      this.errors.remove(smallest)
      this.nodes.remove(smallest)
    }
  }
  
  
  // temperature
  def temperature() = {
    0.3
  }

  // the K-Neighborhood function
  def kNeighbor(neuron1: prototype, neuron2: prototype, T: Double): Double = {
    exp(-(1) / T) //the vicinity !
   }
  
  // the K-Neighborhood function
  def kNeighborSOM(neuron1: prototype, neuron2: prototype, T: Double): Double = {
    exp(-(squaredDistance(neuron1.protoPartNum, neuron2.protoPartNum)) / T)
   }
  
  
  // add a new line and a new column containing each an array of element e 
  def addElementLast(e: Double, a: ArrayBuffer[ArrayBuffer[Double]]) = {
    var b = a
      if (a.size == 0) b.+=(ArrayBuffer.fill(1)(e))
      else {
        b.+=(ArrayBuffer.fill(a(0).size)(e))
        b.foreach { x => x.+=(e) }
        b
      }        
    }
  
  def addElementLast(e: Int, a: ArrayBuffer[ArrayBuffer[Int]]) = {
    var b = a
      if (a.size == 0) b.+=(ArrayBuffer.fill(1)(e))
      else {
        b.+=(ArrayBuffer.fill(a(0).size)(e))
        b.foreach { x => x.+=(e) }
        b
      }        
    }  
  

  // remove the ith line and the ith column 
  def removeLineCol(i: Int, a: ArrayBuffer[ArrayBuffer[Double]]): ArrayBuffer[ArrayBuffer[Double]] = {
    var b = a
        b.remove(i)
        b.foreach { x => x.remove(i) }
        b
    }
  
  def removeLineColInt(i: Int, a: ArrayBuffer[ArrayBuffer[Int]]): ArrayBuffer[ArrayBuffer[Int]] = {
    var b = a
        b.remove(i)
        b.foreach { x => x.remove(i) }
        b
    }
  
  //	res = a x b 
  def scal(a: Double, b: Array[Double]): Array[Double] = {
   b.zipWithIndex.collect{case(x,y) => a * x} 
  }
  def scal(a: Double, b: ArrayBuffer[Double]): ArrayBuffer[Double] = {
   b.zipWithIndex.collect{case(x,y) => a * x} 
  }
  def scal(a: Double, b: Vector[Double]): Vector[Double] = {
    Vector(scal(a, b.toArray))
  }  

  //	res = a + b 
  def addPairwise(a: Array[Double], b: Array[Double]): Array[Double] = {
    (a zip b).map{ case (x, y) => x + y }
  }
  def addPairwiseInt(a: Array[Int], b: Array[Int]): Array[Int] = {
    (a zip b).map{ case (x, y) => x + y }
  }
  def addPairwise(a: ArrayBuffer[Double], b: ArrayBuffer[Double]): ArrayBuffer[Double] = {
    (a zip b).map{ case (x, y) => x + y }
  }
  
  //	res = a*x+y 
  def axpy(a: Double, x: Array[Double], y: Array[Double]) = {
    addPairwise(scal(a, x), y)
  }
  def axpy(a: Double, x: Vector[Double], y: Vector[Double]) = {
    x :*= a
    x + y
  }
  def axpy(a: Int, x: Vector[Int], y: Vector[Int]) = {
    x :*= a
    x + y
  }  

  
  // test if two vectors are equals
  def areQuasiEqual(a: Vector[Double], b: Vector[Double], epsilon: Double = 1e-10):Boolean = {
  var ok=true; 
  if (a.length != b.length) ok=false
  else
    {var i=0;
    while (ok & (i<a.length)){
      if (math.abs(a(i)-b(i)) > epsilon) ok=false
      i+=1
    }
    }
    ok
  } 
  
  
  // toString methods
  override def toString = this.nodes.toArray.deep.mkString("\n")
  def toStringIds = this.nodes.map(_.toStringIds)
  def toStringOutdatedIds = this.outdatedNodes.map(_.toStringIds)  
  def toStringProto = this.nodes.map(_.toStringProto)
  def toStringCard = this.nodes.map(_.toStringCard)
  def toStringAss = this.nodes.map(_.toStringAss)
  def toStringOutdatedAss = this.outdatedNodes.map(_.toStringAss)
  def toStringOutdatedProto = this.outdatedNodes.map(_.toStringProto)
  def toStringIdNode = this.nodes.map(_.toStringId)
  def toStringIdOutdated = this.outdatedNodes.map(_.toStringId)

}
