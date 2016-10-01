/**
 * Put your copyright and license info here.
 */
package com.example.RealtimeSentimentAnalysis;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class TweetReader extends BaseOperator implements InputOperator
{
  private int numTuples = 10;
  private transient int count = 0;
  private transient BufferedReader br;
  private String twitterDataPath;

  public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

  @Override
  public void setup(OperatorContext context)
  {
    try {
      br = new BufferedReader(new FileReader(twitterDataPath));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      try {
        out.emit(br.readLine());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

  public String getTwitterDataPath()
  {
    return twitterDataPath;
  }

  public void setTwitterDataPath(String twitterDataPath)
  {
    this.twitterDataPath = twitterDataPath;
  }
}
