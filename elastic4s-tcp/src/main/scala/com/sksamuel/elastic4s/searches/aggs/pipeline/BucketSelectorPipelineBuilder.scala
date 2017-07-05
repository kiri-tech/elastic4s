package com.sksamuel.elastic4s.searches.aggs.pipeline

import com.sksamuel.elastic4s.ScriptBuilder
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder

object BucketSelectorPipelineBuilder {

  import scala.collection.JavaConverters._

  def apply(p: BucketSelectorDefinition): BucketSelectorPipelineAggregationBuilder = {
    val builder = PipelineAggregatorBuilders.bucketSelector(p.name, ScriptBuilder.apply(p.script), p.bucketsPaths: _*)
    if (p.metadata.nonEmpty) builder.setMetaData(p.metadata.asJava)
    p.gapPolicy.foreach(builder.gapPolicy)
    builder
  }
}
