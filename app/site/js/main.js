queryParams = new URLSearchParams(location.search);
form = document.getElementById("form-search");
input = document.getElementById("query");
if (queryParams.get("query") !== null) {
  input.value = queryParams.get("query");
}
