package cis5550.search;

import static cis5550.search.Utils.parseQuery;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDDImpl;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Job {

  private static final boolean DEBUG = true;

  private static final int CACHE_LIFETIME_MINS = 15;

  private static final ScheduledThreadPoolExecutor cleanupThreads = new ScheduledThreadPoolExecutor(
    1);

  static {
    cleanupThreads.setMaximumPoolSize(4);
  }

  public static void run(FlameContext ctx, String[] args) throws Exception {
    if (args.length < 1) {
      ctx.output("missing query argument");
      return;
    }
    String query = args[0];
    List<String> tokenizedQuery = parseQuery(query).sorted().toList();
    String queryKey = URLEncoder.encode(tokenizedQuery.toString(), StandardCharsets.UTF_8);
    System.out.println(queryKey);
    KVSClient kvsMaster = ctx.getKVS();
    if (!kvsMaster.existsRow("cached", queryKey)) {
      FlameRDD queryRDD = ctx.parallelize(tokenizedQuery);
      if (DEBUG) {
        queryRDD.saveAsTable("query");
      }
      FlamePairRDD urlsRDD = queryRDD.flatMapToPair(
        word -> () -> getIndex(word, ctx.getKVS()).map(url -> url.replaceAll(":(?:\\d*\\s*)*$", ""))
          .map(url -> new FlamePair(url, "1")).iterator());
      if (DEBUG) {
        urlsRDD.saveAsTable("urls");
      } else {
        queryRDD.drop();
      }
      FlamePairRDD urlCounts = urlsRDD.foldByKey("0",
        (n1, n2) -> String.valueOf(Integer.parseInt(n1) + Integer.parseInt(n2)));
      if (DEBUG) {
        urlCounts.saveAsTable("urlCounts");
      } else {
        urlsRDD.drop();
      }
      String invertedRankTable = !DEBUG ? UUID.randomUUID().toString() : "invertedRanks";
      FlameRDD invertedRanks = urlCounts.flatMap(urlCount -> {
        KVSClient kvs = ctx.getKVS();
        kvs.put(invertedRankTable, String.valueOf(
          (10 - Double.parseDouble(kvs.getRow("pageranks", urlCount._1()).get("rank")))
          / Integer.parseInt(urlCount._2())), "url", urlCount._1());
        return Collections::emptyIterator;
      });
      if (!DEBUG) {
        urlCounts.drop();
      }
      int i = 0;
      int paddingLen = String.valueOf(kvsMaster.count(invertedRankTable)).length() + 1;
      for (Iterator<Row> it = kvsMaster.scan(invertedRankTable); it.hasNext(); i++) {
        Row row = it.next();
        try {
          kvsMaster.put(queryKey, String.format("%0" + paddingLen + "d", i), "url", row.get("url"));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      if (!DEBUG) {
        kvsMaster.delete("invertedRankTable");
        invertedRanks.drop();
      }
      kvsMaster.put("cached", queryKey, "table", queryKey);
    } else {
      FlamePairRDD cacheEntries = new FlamePairRDDImpl((FlameContextImpl) ctx, "cached");
      FlamePairRDD newCacheEntries = cacheEntries.flatMapToPair(ent -> {
        if (System.currentTimeMillis() - Long.parseLong(ent._2()) > CACHE_LIFETIME_MINS * 1000) {
          try {
            kvsMaster.delete(ent._1());
          } catch (IOException ignored) {
          }
          return Collections::emptyIterator;
        } else {
          return List.of(new FlamePair(ent._1(), String.valueOf(System.currentTimeMillis())));
        }
      });
      cacheEntries.drop();
      newCacheEntries.saveAsTable("cached");
    }
  }

  private static String getPage(String url, KVSClient kvsClient) {
    try {
      return new String(kvsClient.get("crawl", url, "page"), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "";
    }
  }

  private static Stream<String> getIndex(String keyword, KVSClient kvsClient) {
    if (keyword.matches("^\".*\"$")) {
      String kw = keyword.replaceAll("^\"|\"$", "");
      return Arrays.stream(kw.split("\\s")).map(String::translateEscapes)
        .flatMap(w -> getIndex(w, kvsClient)).filter(
          url -> Pattern.compile(Pattern.quote(kw), Pattern.CASE_INSENSITIVE)
            .matcher(getPage(url.replaceAll(":(?:\\d*\\s*)*$", ""), kvsClient)).find());
    }

    try {
      Row entry = kvsClient.getRow("index", keyword);
      if (entry != null) {
        return entry.columns().stream().map(entry::get).flatMap(e -> Arrays.stream(e.split(",")));
      } else {
        return Stream.empty();
      }
    } catch (IOException e) {
      return Stream.empty();
    }
  }

}
