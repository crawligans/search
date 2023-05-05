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
import cis5550.tools.Logger;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Job {

  private static final boolean DEBUG = true;

  private static final int CACHE_LIFETIME_MINS = 15;

  private static final ScheduledThreadPoolExecutor cleanupThreads = new ScheduledThreadPoolExecutor(
    1);

  static {
    cleanupThreads.setMaximumPoolSize(4);
  }

  public static void run(FlameContext ctx0, String[] args) throws Exception {
    if (args.length < 1) {
      ctx0.output("missing query argument");
      return;
    }
    String query = args[0];
    List<String> tokenizedQuery = parseQuery(query).sorted().toList();
    String queryKey = URLEncoder.encode(tokenizedQuery.toString(), StandardCharsets.UTF_8);

    final int Q_LEN = tokenizedQuery.size();
    FlameContextImpl ctx = (FlameContextImpl) ctx0;
    KVSClient kvsMaster = ctx.getKVS();
    //if (!kvsMaster.existsRow("cached", queryKey)) {
      //System.out.println("om");
      FlamePairRDD queryTok = ctx.parallelizePairs(
        IntStream.range(0, Q_LEN).mapToObj(i -> new FlamePair(
          tokenizedQuery.get(i), String.valueOf(i))));

      queryTok.saveAsTable("queryTok");

      FlamePairRDD cms = queryTok.flatMapToPair(q -> {
        System.out.println(q);
        final int idx = Integer.parseInt(q._2());
        return () -> getIndex(q._1(), ctx.getKVS()).map(url -> {
          System.out.println(url);
          int tf = url.substring(url.lastIndexOf(':')).split(" ").length;
          String urlStr = url.substring(0, url.lastIndexOf(':'));
          String[] pos = new String[Q_LEN];
          Arrays.fill(pos, "0");
          pos[idx] = String.valueOf(tf);
          String vals = String.join(",", pos);
          return new FlamePair(urlStr, vals);
        }).iterator();
      });
      cms.saveAsTable("cms");
      String[] initVals = new String[Q_LEN];
      Arrays.fill(initVals, "0");
      cms.foldByKey(String.join(",", initVals), (acc, url) -> {
        String[] vals = acc.split(",");
        String[] cld = url.split(",");
        assert cld.length == vals.length;
        for (int i = 0; i < cld.length; i++) {
          if (!cld[i].equals("0")) {
            vals[i] = String.valueOf(Integer.parseInt(cld[i]) + Integer.parseInt(vals[i]));
          }
        }
        return String.join(",", vals);
      }).flatMapToPair(flamePair -> {
        String url = flamePair._1();
        String line = flamePair._2();
        String[] cvals = line.split(",");
        for (int i = 0; i < Q_LEN; i++) {
          String qWordAndIndex = tokenizedQuery.get(i);
          String qWord = qWordAndIndex.substring(qWordAndIndex.indexOf('=')+1);
          double j = Double.parseDouble(new String(ctx.getKVS().get("idfRanks", qWord , "IDF")))
                     * Integer.parseInt(cvals[i]);
          cvals[i] = Double.toString(j);
        }
        return Collections.singletonList(new FlamePair(url, String.join(",", cvals)));
      }).saveAsTable("rankTable");

      kvsMaster.put("cached", queryKey, "table", queryKey);
      cleanupThreads.schedule(() -> {
        try {
          refreshCache(ctx);
        } catch (Exception e) {
          Logger.getLogger(Job.class).error(e.getMessage(), e);
        }
      }, CACHE_LIFETIME_MINS, TimeUnit.MINUTES);
    //}
  }

  private static void refreshCache(FlameContext ctx) throws Exception {
    FlamePairRDD cacheEntries = new FlamePairRDDImpl((FlameContextImpl) ctx, "cached");
    FlamePairRDD newCacheEntries = cacheEntries.flatMapToPair(ent -> {
      if (System.currentTimeMillis() - Long.parseLong(ent._2()) > CACHE_LIFETIME_MINS * 1000) {
        try {
          ctx.getKVS().delete(ent._1());
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

  private static String getPage(String url, KVSClient kvsClient) {
    try {
      return new String(kvsClient.get("crawl", url, "page"), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "";
    }
  }

  private static Stream<String> getIndex(String keyword, KVSClient kvsClient) {
    /*if (keyword.matches("^\".*\"$")) {
      String kw = keyword.replaceAll("^\"|\"$", "");
      return Arrays.stream(kw.split("\\s")).map(String::translateEscapes)
        .flatMap(w -> getIndex(w, kvsClient)).filter(
          url -> Pattern.compile(Pattern.quote(kw), Pattern.CASE_INSENSITIVE)
            .matcher(getPage(url.replaceAll(":(?:\\d*\\s*)*$", ""), kvsClient)).find());
    }*/
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
