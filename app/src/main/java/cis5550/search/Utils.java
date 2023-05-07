package cis5550.search;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Utils {

  private static final Pattern quoted = Pattern.compile(
    "(?<=\\b)\"(?:\\Q\\\"\\E|[^\"])*?\"(?=\\b)");

  public static Stream<String> parseQuery(String q) {
    StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(q));
    tokenizer.quoteChar('"');
    tokenizer.lowerCaseMode(true);
    return Stream.generate(() -> {
      try {
        tokenizer.nextToken();
        return tokenizer.ttype == '"' ? '"' + tokenizer.sval + '"' : tokenizer.sval;
      } catch (IOException e) {
        return "";
      }
    }).takeWhile(Objects::nonNull).filter(Predicate.not(String::isBlank));
  }

  public static String stem(String word) {
    Stemmer stemmer = new Stemmer();
    char[] wordArr = word.toCharArray();
    stemmer.add(wordArr, wordArr.length);
    stemmer.stem();
    return stemmer.toString();
  }
}
