package com.microsoft.hdinsight.storm.examples;

import java.io.Serializable;
import java.util.Random;

public class MessageGenerator implements Serializable {

  private static final long serialVersionUID = 2628073895157236324L;
  private static final char[] ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".toCharArray();

  private final Random random;
  private final int length;
  private final boolean cache;

  private String cachedMessage;

  public MessageGenerator(int length, boolean cache) {
    this.random = new Random();
    this.length = length;
    this.cache = cache;
  }

  public String getNextMessage() {
    if (cache) {
      if (cachedMessage == null) {
        cachedMessage = createMessage();
      }
      return cachedMessage;
    }
    return createMessage();
  }

  private String createMessage() {
    char[] message = new char[length];
    for (int i = 0; i < length; i++) {
      int index = random.nextInt(ALPHABET.length);
      message[i] = ALPHABET[index];
    }
    return new String(message);
  }
}
