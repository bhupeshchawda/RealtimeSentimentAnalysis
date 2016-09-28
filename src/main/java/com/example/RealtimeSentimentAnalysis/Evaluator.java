package com.example.RealtimeSentimentAnalysis;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class Evaluator extends BaseOperator
{
  private long totalTweets = 0;
  private long correctlyEvaluated = 0;

  public final transient DefaultInputPort<String> inputPositive = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      totalTweets++;
      evaluateTuple(tuple, 1);
    }
  };

  public final transient DefaultInputPort<String> inputNegative = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      totalTweets++;
      evaluateTuple(tuple, 0);
    }
  };

  public void evaluateTuple(String tuple, int sentiment)
  {
    String[] fields = tuple.split("\\|");
    int actualSentiment = Integer.parseInt(fields[1].trim());
    if (actualSentiment == sentiment) {
      correctlyEvaluated++;
    }
  }

  @Override
  public void endWindow()
  {
    System.out.println("Accuracy: " + (correctlyEvaluated/(totalTweets*1.0)));
  }
}
