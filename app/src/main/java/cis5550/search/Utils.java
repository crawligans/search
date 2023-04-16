package cis5550.search;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Utils {

  private static final Pattern keywords = Pattern.compile(
    "(?<=\\b)(\"(?:\\Q\\\"\\E|[^\"])+\"|[-\\w]+)(?=\\b)");

  public static Stream<String> parseQuery(String q) {
    return keywords.matcher(URLDecoder.decode(q, StandardCharsets.UTF_8)).results()
      .map(MatchResult::group);
  }

}
