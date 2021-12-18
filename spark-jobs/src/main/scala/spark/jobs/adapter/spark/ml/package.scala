package spark.jobs.adapter.spark

import org.apache.spark.ml.Pipeline

package object ml {
  implicit class SparkMLComponentSyntax(component: Array[SparkMLComponent]) {

    /**
     * Used to connect SparkMLComponents.
     */
    def ~>(that: SparkMLComponent): Array[SparkMLComponent] =
      component :+ that

    /**
     * Used to specify the last SparkMLComponent in the pipeline.
     */
    def >->(that: SparkMLComponent): Pipeline =
      new Pipeline().setStages((component ~> that).map(_.inner))
  }
}
