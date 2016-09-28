/**
 * Put your copyright and license info here.
 */
package com.example.RealtimeSentimentAnalysis;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TweetReader tweetReader = dag.addOperator("TweetReader", TweetReader.class);
    SentimentClassifier classifier = dag.addOperator("SentimentClassifier", SentimentClassifier.class);
    Evaluator evaluator = dag.addOperator("Evaluator", Evaluator.class);

    dag.addStream("Reader To Classifier", tweetReader.out, classifier.input);
    dag.addStream("Classifier To Evaluator Potitive", classifier.positive, evaluator.inputPositive);
    dag.addStream("Classifier To Evaluator Negative", classifier.negative, evaluator.inputNegative);
  }
}
