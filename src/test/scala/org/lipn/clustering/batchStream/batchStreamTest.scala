package org.lipn.clustering.batchStream
	 /**
	  * Copyright: please refer to the README.md file
	  * User: ghesmoune
	  * Date: 01/01/2016
	  * Project : Square Predict (http://square-predict.net/)
	  * */ 
import org.junit.Test

class GStreamTest {
  @Test def runAlgo {

    val params = Array("local[2]", "conf/test/resources", "conf/test/results", "DS1-200", ",", "0.9", "1.2", "3", "91")
    
    val stack = batchStreamRun.main(params)
    
  }
}
