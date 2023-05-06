package cis5550.search;

import static cis5550.search.Utils.parseQuery;
import static java.util.stream.StreamSupport.stream;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.webserver.Response.Status;
import cis5550.webserver.Server;
import com.google.gson.Gson;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Search <port> <KVSMaster> <FlameMaster>");
      return;
    }

    int port = Integer.parseInt(args[0]);
    Server.port(port);

    KVSClient kvs = new KVSClient(args[1]);
    String flame = args[2];

    Server.staticFiles.location(System.getProperty("user.dir"));
    Server.get("/", (req, res) -> {
      String queries = req.queryParams().stream().map(
          k -> "%s=%s".formatted(URLEncoder.encode(k, StandardCharsets.UTF_8),
            URLEncoder.encode(req.queryParams(k), StandardCharsets.UTF_8)))
        .collect(Collectors.joining("&"));
      if (req.queryParams("query") != null) {
        res.header("Location", "/search?%s".formatted(queries));
      } else {
        res.header("Location", "/index.html?%s".formatted(queries));
      }
      res.status(Status.SEE_OTHER);
      return "Redirecting...";
    });

    Server.get("/search", (req, res) -> {
      String query = req.queryParams("query");
      if (query == null) {
        String queryString = req.queryParams().stream().map(
            k -> "%s=%s".formatted(URLEncoder.encode(k, StandardCharsets.UTF_8),
              URLEncoder.encode(req.queryParams(k), StandardCharsets.UTF_8)))
          .collect(Collectors.joining("&"));
        res.status(Status.SEE_OTHER);
        res.header("Location", "/index.html?%s".formatted(queryString));
        return null;
      }
      String format = req.queryParams("f");
      int fromIdx =
        req.queryParams("fromIdx") != null ? Integer.parseInt(req.queryParams("fromIdx")) : 0;
      List<String> tokenizedQuery = parseQuery(query).sorted().toList();
      String queryKey = Hasher.hash(tokenizedQuery.toString());
      try {
        if (!kvs.existsRow("cached", queryKey)) {
          FlameSubmit.submit(flame,
            Job.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath(),
            Job.class.getName(), new String[]{query});
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      Map<String, Object> resultMetadata = toJsonResponse(stream(((Iterable<Row>) () -> {
        try {
          return kvs.scan(queryKey, String.valueOf(fromIdx), null);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).spliterator(), false).map(r -> r.get("url")), fromIdx, kvs);
      if ("json".equalsIgnoreCase(format)) {
        return new Gson().toJson(resultMetadata);
      } else {
        return buildPage(resultMetadata);
      }
    });
  }

  private static String buildPage(Map<String, Object> fetchedMetadata) {
    return """
      <!doctype html>
      <html class="no-js" lang="">

      <head>
        <meta charset="utf-8">
        <title></title>
        <meta name="description" content="">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha3/dist/css/bootstrap.min.css"
              rel="stylesheet"
              integrity="sha384-KK94CHFLLe+nY2dmCWGMq91rCGa5gtU4mk92HdvYe+M/SXH301p5ILy+dN9+nJOZ"
              crossorigin="anonymous">
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha3/dist/js/bootstrap.bundle.min.js"
                integrity="sha384-ENjdO4Dr2bkBIFxQpeoTz1HIcje39Wm4jDKdf19U8gI4ddQ3GYNS7NTKfAdVQSZe"
                crossorigin="anonymous"></script>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.4/font/bootstrap-icons.css">
        <meta property="og:title" content="">
        <meta property="og:type" content="">
        <meta property="og:url" content="">
        <meta property="og:image" content="">

        <link rel="manifest" href="site.webmanifest">
        <link rel="apple-touch-icon" href="icon.png">
        <!-- Place favicon.ico in the root directory -->

        <link rel="stylesheet" href="css/normalize.css">
        <link rel="stylesheet" href="css/main.css">

        <meta name="theme-color" content="#fafafa">
      </head>

      <body>
      <div class="px-5 my-4 container-sm">
        <form class="row g-2 sticky-top-sm my-4" id="form-search" action="/search">
          <div class="col-auto flex-grow-1">
            <label for="query" class="visually-hidden">keywords</label>
            <input type="text" id="query" name="query" class="form-control" placeholder="keywords">
          </div>
          <div class="col-auto">
            <button type="submit" class="btn btn-primary"><i class="bi bi-search"></i></button>
          </div>
        </form>
        <div class="vstack gap-sm-2">
          %s
        </div>
      </div>
      <script src="js/vendor/modernizr-3.11.2.min.js"></script>
      <script src="js/plugins.js"></script>
      <script src="js/main.js"></script>

      </body>

      </html>
      """.formatted(
      ((List<Map<String, String>>) fetchedMetadata.get("results")).stream().map(entry -> """
          <div>
            <div class="position-relative">
              <h3 class="h3" style="display: inline-block">%s</h3>
              <a class="link-opacity-100 stretched-link" href="%s">%s</a>
            </div>
            <p class="position-relative card-subtitle">%s
            </p>
          </div>
        """.formatted(entry.get("title"), entry.get("url"), entry.get("url"),
        entry.get("description"))).collect(Collectors.joining("\n")));
  }

  private static Map<String, Object> toJsonResponse(Stream<String> urls, int fromIdx,
    KVSClient kvs) {
    Map<String, Object> resp = new HashMap<>();
    resp.put("fromIdx", fromIdx);
    resp.put("results", urls.map(url -> {
      Map<String, String> entry = new HashMap<>();
      entry.put("url", url);
      try {
        byte[] pageBytes = kvs.get("crawl", Hasher.hash(url), "page");
        if (pageBytes == null) return null;
        String page = new String(pageBytes);
        Pattern title = Pattern.compile("<title.*?>(.*)</\\s*?title\\s*?>", Pattern.DOTALL);
        Matcher titleMatcher = title.matcher(page);
        if (titleMatcher.find()) {
          String ttl = titleMatcher.group(1);
          entry.put("title", !ttl.isBlank() ? ttl : "untitled page");
        } else {
          entry.put("title", "untitled page");
        }
        Pattern metaDesc = Pattern.compile(
          "<meta\\s+name=\"description\"\\s+content=(\"(?:\\Q\\\"\\E|[^\"]*)\"|'.*')\\s+/?>(?:</meta>)?",
          Pattern.DOTALL);
        Matcher descMatcher = metaDesc.matcher(page);
        if (descMatcher.find()) {
          entry.put("description", descMatcher.group(1));
        } else {
          Pattern body = Pattern.compile("<body.*?>(.*)</\\s*body\\s*>", Pattern.DOTALL);
          Matcher pageMatcher = body.matcher(page);
          if (pageMatcher.find()) {
            String bodyText = pageMatcher.group(1);
            String innerText =
              bodyText.replaceAll("(?s)<.*?>", " ").replaceAll("\\s+", " ");
            entry.put("description",
              innerText.substring(0, Math.min(innerText.length(), 1024)) + "...");
          } else {
            entry.put("description", "not available");
          }
        }
      } catch (IOException e) {
        return null;
      }
      return entry;
    }).filter(Objects::nonNull).toList());
    return resp;
  }
}
