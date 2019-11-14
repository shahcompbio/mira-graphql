import bodybuilder from "bodybuilder";

import client from "../api/elasticsearch.js";

export default async function getSamples(type, dashboardID) {
  if (type === "sample") {
    return [dashboardID];
  } else {
    const query = bodybuilder()
      .size(10000)
      .filter("term", "dashboard_id", dashboardID)
      .build();

    const result = await client.search({
      index: "dashboard_entry",
      body: query
    });

    return result["hits"]["hits"][0]["_source"]["sample_ids"];
  }
}
