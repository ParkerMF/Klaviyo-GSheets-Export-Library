/**
 * Fetches IDs for all metrics in the account, returning the results with a column for Metric Name, ID, and Integration
 *
 * @param {apiKey} Klaviyo private API key.
 * @return Returns all unique metrics associated with the corresponding Klaviyo account.
 * @customfunction
 */
function klGetMetricIds(apiKey) {
  var requestUrl = "https://a.klaviyo.com/api/v1/metrics?api_key=" + apiKey;
  var jsonData = ImportJSON(requestUrl, "/data/name,/data/id,/data/integration/name","noHeaders");
  // Rename the columns to be more readable
  var colNames = [["Name", "Id", "Integration"]];
  jsonData = colNames.concat(jsonData);

  return jsonData;
}


/**
 * Fetches IDs for a particular metric matching on name. If there is more than one by the same name, it will return only the first.
 *
 * @param {apiKey} Klaviyo private API key.
 * @param {metricName} The name of a Klaviyo metric to match on (case-sensitive).
 * @param {integrationName} The name of an integration associated with the metric (case-sensitive). Use this if more than one result may be returned based on metric name match.
 * @return Fetches IDs for a particular metric matching on name. First result will be returned if there is more than one by the same name unless an integration name is specified.
 * @customfunction
 */
function klGetMetricIdByName(apiKey, metricName, integrationName) {
  var jsonData = klGetMetricIds(apiKey);
  if (integrationName == null) {
    // Filter for rows where the first column matches metricName
    var filtered = jsonData.filter(function(row){
    return row[0] === metricName;
    });
    // Filter for only the second column, which contains the metric ID
    var id  = filtered.map(function(value,index) { return value[1]; });
  return(id);
  }
    // Filter for rows where the first column matches metricName and the third matches integrationName
    var filtered = jsonData.filter(function(row){
    return row[0] === metricName && row[2] === integrationName;
    });
    // Filter for only the second column, which contains the metric ID
    var id  = filtered.map(function(value,index) { return value[1]; });

  return id;
}


/**
 * Fetches data from the Metrics Export API, given a metric ID, API key, and other optional parameters.
 *
 * @param {apiKey} Klaviyo private API key.
 * @param {metricId} The ID of a Klaviyo metric to match on (case-sensitive). Use the klGetMetricIds() or klGetMetricIdByName() function if needed to find the appropriate ID.
 * @param {startDate} Beginning of timeframe to pull event data for. The default value is 1 month ago. Uses YYYY-MM-DD formatting, without quotes, e.g.: 2020-06-15
 * @param {endDate} End of timeframe to pull event data for. The default is the current day. Uses YYYY-MM-DD formatting, without quotes, e.g.: 2020-06-15
 * @param {unit} Granularity to bucket data points into. Specify: day, week, or month (case-sensitive). Defaults to day.
 * @param {measurement} Type of metric to fetch. Specify: unique, count, value, or sum (case-sensitive). Defaults to count. For sum a property name to operate on must be supplied as a JSON-encoded list like '["sum","ItemCount"]'.
 * @param {where} Conditions to use to filter the set of events. A max of 1 condition can be given. Where and by parameters cannot be specified at the same time. Illustrative example: [["ItemCount","=",5]]
 * @param {by} The name of a property to segment the event data on. Where and by parameters cannot be specified at the same time.
 * @param {count} Maximum number of segments to return. The default value is 25.
 * @param {response} How the function should respond, either: data or url (case-sensitive). If url is specified the URL for the API request is returned. The default value is data.
 * @return Fetches data from the Metrics Export API, given a metric ID, API key, and other optional parameters.
 * @customfunction
 */
function klMetricExport(apiKey, metricId, startDate, endDate, unit, measurement, where, by, count, includeDateColumn, includeValuesColumn, includeSegmentColumn, includeHeaders) {
  // Throw alert if required arguments aren't provided or are obviously invalid
  if (apiKey === undefined || apiKey === null || apiKey === "") {
    throw 'Error: You must include an API key'
  }
  if (metricId === undefined || metricId === null || metricId === "") {
    throw 'Error: You must include a Metric ID'
  }
  if (typeof metricId !== 'string' || (typeof metricId === 'string' && (metricId.length > 6 || metricId.length < 6))) {
    throw 'Error: Metric ID must be a six character string'
  }

  // Set defaults for optional arguments
  if (startDate === undefined || startDate === null || startDate === "") {
    var date = new Date();
    date.setDate(date.getDate()-30);
    startDate = Utilities.formatDate(date, "GMT", "yyyy-MM-dd");
  }

  if (endDate === undefined || endDate === null || endDate === "") {
    endDate = Utilities.formatDate(new Date(), "GMT", "yyyy-MM-dd");
  }

  if (unit === undefined || unit === null || unit === "") {
    unit = "day"
  }

  if (measurement === undefined || measurement === null || measurement === "") {
    measurement = "count"
  }

  if (count === undefined || count === null || count === "") {
    count = "25"
  }

  if (includeDateColumn === undefined || includeDateColumn === null || includeDateColumn === "") {
    includeDateColumn = 1
  }

  if (includeValuesColumn === undefined || includeDateColumn === null || includeValuesColumn === "") {
    includeValuesColumn = 1
  }

  if (includeSegmentColumn === undefined || includeDateColumn === null || includeSegmentColumn === "") {
    includeSegmentColumn = 1
  }

  if (includeHeaders === undefined || includeDateColumn === null || includeHeaders === "") {
    includeHeaders = 1
  }

  var baseRequest = "https://a.klaviyo.com/api/v1/metric/" + metricId + "/export?api_key=" + apiKey + "&start_date=" + startDate + "&end_date=" + endDate + "&unit=" + unit +"&measurement=" + measurement + "&count=" + count;

  // Determine which columns to output
  var queryParam = []
  if (includeDateColumn == 0 && includeValuesColumn == 0 && includeSegmentColumn == 0) {
    throw 'Error: IncludeDataColumn, includeValuesColumn, and includeSegmentColumn cannot all be 0'
  }
  if (includeDateColumn == 0) {
  } else {
    queryParam.push("/results/data/date");
  }
  if (includeValuesColumn == 0) {
  } else {
    queryParam.push("/results/data/values");
  }
  if (includeSegmentColumn == 0) {
  } else {
    queryParam.push("/results/data/segment");
  }
  queryParam = queryParam.join();

  // Determine whether to output column names
  if (includeHeaders == 0) {
    var headersParam = "noHeaders";
  } else {
    headersParam = null;
  }

  // Extend base request when "where" or "by" are supplied.
  // Throw error if both are supplied
  if ((where !== null && where !== "" && where !== undefined) && (by !== null && by !== "" && by !== undefined)) {
    throw 'Error:  Where and by parameters cannot be specified at the same time'
  }
  // Else if "where" has a non-null value, use the request that contains a "where" parameter
  else if (where !== null && where !== "" && where !== undefined) {
    var requestUrl = baseRequest + "&where=" + URLEncode(where);
    var jsonData = klMetricExportByUrl(requestUrl, queryParam, headersParam);
  }
  // Else if "by" has a non-null value, use the request that contains a "by" parameter
  else if (by !== null && by !== "" && by !== undefined) {
    var requestUrl = baseRequest + "&by=" + URLEncode(by);
    var jsonData = klMetricExportByUrl(requestUrl, queryParam, headersParam);
  }
  // Else use the base request (which has neither a "by" nor "where" parameter)
  else {
    var requestUrl = baseRequest;
    var jsonData = klMetricExportByUrl(requestUrl, queryParam, headersParam);
  }

  return jsonData;

}


/**
 * Given a properly-formatted request URL to the Metrics Export API, fetches data and flattens it to a 2D array.
 * Adapted version of ImportJSON() function to handle the special case of responses from the Metrics Export API.
 * We use the helper function transformMetricsExportResponse() to manipulate the JSON object to have the desired structure.
 * Then we use parseJSONObject_() from the ImportJSON library to parse the JSON object.
 *
 * @param {requestUrl} Properly formatted request URL including a valid API key. Optionally, you can use klMetricExport() to build such a URL.
 * @param {query} Specifies which fields to include in the results, using the raw header values. Defaults to: "/results/data/date,/results/data/values,/results/data/segment"
 * @param {options} Optional parameters. See ImportJSON() for more details. Defaults to null.
 * @return Given a properly-formatted request URL to the Metrics Export API, fetches data and flattens it to a 2D array.
 * @customfunction
 */
function klMetricExportByUrl(requestUrl, query, options) {
  if (query === null || query === "" || query === undefined) {
    query = "/results/data/date,/results/data/values,/results/data/segment"
  }
  var response = UrlFetchApp.fetch(requestUrl);
  var object = JSON.parse(response.getContentText());
  var objectTransformed = JSON.parse(transformMetricsExportResponse(object));

  return parseJSONObject_(objectTransformed, query, options, includeXPath_, defaultTransform_);
}


/**
 * Returns helpful illustrative requests for reference. These requests can then be evaluated using klMetricExportByUrl().
 *
 * @param {apiKey} Klaviyo private API key.
 * @return Returns helpful requests for reference. These requests can then be evaluated using klMetricExportByUrl().
 * @customfunction
 */
function klHelpfulReferenceRequests(apiKey) {
  var date = new Date();
  date.setDate(date.getDate()-30);
  var startDate = Utilities.formatDate(date, "GMT", "yyyy-MM-dd");
  var endDate = Utilities.formatDate(date, "GMT", "yyyy-MM-dd");
  var unit = "day";
  var measurement = "count";
  var count = 25;
  var baseUrl = "https://a.klaviyo.com/api/v1/metric/";

  var popularProducts = ["Returns Placed Orders over past 30 days, split by Product", baseUrl + klGetMetricIdByName(apiKey, "Placed Order") + apiKey + "&start_date=" + startDate + "&end_date=" + endDate + "&unit=" + unit + "&measurement=" + "value" + "&count=" + count];

  return [popularProducts];

}


/**
 * Uses regular expression to extract the 6 character ID from a cell with contents like this: Email #1 (WTLSUQ).
 * @param {input} Cell contents to evaluate for message ID extraction.
 * @return Uses regular expression to extract the 6 character ID from a cell with contents like this: Email #1 (WTLSUQ).
 * @customfunction
 */
function klExtractMessageId(input) {
  var regExp = /\(([^()]*)\)/;
  var output = regExp.exec(input);
  return output[0];
}


/**
 * Helper function that parses the JSON response from the Metrics Export API and nests "segment" alongside "date" and "value" for each returned "data" object.
 * This is because the Metrics Export API returns an array of "data" objects, each containing a key-value pair for "date" and an array for "value".
 * A value for "segment" is also supplied - this identifies how "data" has been grouped (or reports "everyone" if no "by" clause is included)
 * However, "segment" is adjacent to the "data" object and so when the data is flattened, it does not take the desired form.
 * Example:  https://gist.githubusercontent.com/ParkerMF/724026ec4c9f4810e1ebc6202f9e25c5/raw/160c16f0c52283770aa2d8e5053d1eaaa576e7ae/Metrics%2520Export%2520output%2520sample.json
 * As a result, we need to grab "segment" and nest it within each object in the "data" array.
 * Using the default ImportJSON() function doesn't work for this scenario, so we need a custom function.
 */
const transformMetricsExportResponse = (response) => {
  let output = []
  const formattedData = response.results.forEach((result) =>{
    var mappedResults = result.data.map((datum) => {
      const datumWithSegment = Object.assign({}, datum)
      datumWithSegment['segment'] = result.segment
      return datumWithSegment
    })
    output = output.concat(mappedResults)
  })
  return JSON.stringify({
    results : [
      {
        data : output
      }
    ]
  })
}
