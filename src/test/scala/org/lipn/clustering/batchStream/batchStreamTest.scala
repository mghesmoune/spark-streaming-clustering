package org.lipn.clustering.batchStream
	 /**
	  * Copyright: LIPN, université Paris 13
	  * Projet : Square Predict
	  * << le code est confidentiel et aucun partage n'est autorisé...>> 
	  * */ 
import org.junit.Test

class GStreamTest {
  @Test def runAlgo {

    val params = Array("local[2]", "conf/test/resources", "conf/test/results", "DS1-200", ",", "0.9", "1.2", "3", "91")
    
    val stack = batchStreamRun.main(params)
    
  }
}