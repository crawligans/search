queryParams = new URLSearchParams(location.search);
form = document.getElementById("form-search");
input = document.getElementById("query");
if (queryParams.get("query") !== null) {
  input.value = queryParams.get("query");
}

form.addEventListener("submit", e => {
  if (!input.value) {
    location.assign(
      window.location.protocol + "//" + window.location.host + "/index.html")
  } else {
    location.assign(
      window.location.protocol + "//" + window.location.host + "/search?query="
      + encodeURIComponent(input.value))
  }
});
