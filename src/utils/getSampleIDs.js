import bodybuilder from "bodybuilder";

import client from "../api/elasticsearch.js";

export default async function getSamples(type, dashboardID) {
  if (type === "sample") {
    return [dashboardID];
  } else {
    // For now just assumes that it's patient
    const [patient_id, sort] = dashboardID.split("_");
    const query = bodybuilder()
      .size(10000)
      .filter("term", "patient_id", patient_id)
      .filter("term", "sort", sort)
      .build();

    const result = await client.search({
      index: "sample_metadata",
      body: query
    });

    return result["hits"]["hits"]
      .map(record => record["_source"]["sample_id"])
      .sort();
  }
}
