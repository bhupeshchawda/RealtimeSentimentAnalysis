package com.example.RealtimeSentimentAnalysis;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Lists;

public class SentimentClassifier extends BaseOperator
{
  private transient ArrayList<String> positiveWords = Lists.newArrayList();
  private transient ArrayList<String> negativeWords = Lists.newArrayList();
  private String positiveDictionaryPath;
  private String negativeDictionaryPath;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      String s = "";

      // Cache Positive Words
      BufferedReader br = new BufferedReader(new FileReader(positiveDictionaryPath));
      while((s = br.readLine()) != null) {
        if (! positiveWords.contains(s.toLowerCase()))
        positiveWords.add(s.toLowerCase());
      }
      br.close();

      // Cache Negative Words
      br = new BufferedReader(new FileReader(negativeDictionaryPath));
      while((s = br.readLine()) != null) {
        if (! negativeWords.contains(s.toLowerCase()))
        negativeWords.add(s.toLowerCase());
      }
      br.close();

    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      processTuple(tuple);
    }
  };

  public final transient DefaultOutputPort<String> positive = new DefaultOutputPort<>();

  public final transient DefaultOutputPort<String> negative = new DefaultOutputPort<>();

  private void processTuple(String tuple)
  {
    int positiveCount = 0;
    int negativeCount = 0;

    String[] tokens = tuple.split(" ");
    for (String token: tokens) {
      if (positiveWords.contains(token.trim().toLowerCase())) {
        positiveCount++;
      }
      if (negativeWords.contains(token.trim().toLowerCase())) {
        negativeCount++;
      }
    }

    if(positiveCount > negativeCount) {
      positive.emit(tuple);
    } else if(positiveCount < negativeCount) {
      negative.emit(tuple);
    }
  }

  public String getPositiveDictionaryPath()
  {
    return positiveDictionaryPath;
  }

  public void setPositiveDictionaryPath(String positiveDictionaryPath)
  {
    this.positiveDictionaryPath = positiveDictionaryPath;
  }

  public String getNegativeDictionaryPath()
  {
    return negativeDictionaryPath;
  }

  public void setNegativeDictionaryPath(String negativeDictionaryPath)
  {
    this.negativeDictionaryPath = negativeDictionaryPath;
  }
}
