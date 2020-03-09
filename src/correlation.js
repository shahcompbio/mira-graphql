const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

import { getSampleMap } from "./density";

export const schema = gql`
  extend type Query {
    correlation(
      dashboardID: String!
      labels: [AttributeInput!]
    ): [CorrelationCell!]!
  }

  type CorrelationCell {
    x: StringOrNum!
    y: StringOrNum!
  }
`;

export const resolvers = {
  Query: {
    async correlation(_, { dashboardID, labels }) {
      // Here we assume there's only two labels
      const [xLabel, yLabel] = labels;

      const cellIDs = await getCellIDs(dashboardID);
      const xData = await getData(dashboardID, xLabel);
      const yData = await getData(dashboardID, yLabel);

      return cellIDs.map(cellID => ({ x: xData[cellID], y: yData[cellID] }));
    }
  }
};

async function getCellIDs(dashboardID) {
  const query = bodybuilder()
    .size(50000)
    .filter("term", "dashboard_id", dashboardID)
    .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  return results["hits"]["hits"].map(record => record["_source"]["cell_id"]);
}

async function getData(dashboardID, label) {
  if (label["isNum"]) {
    if (label["type"] === "CELL") {
      return getCellNumericalValue(dashboardID, label);
    } else {
      // is GENE
      return getGeneValue(dashboardID, label);
    }
  } else {
    if (label["type"] === "CELL") {
      return getCelltypeValue(dashboardID, label);
    } else {
      // is sample
      return getSampleValue(dashboardID, label);
    }
  }
}

async function getCellNumericalValue(dashboardID, label) {
  const query = bodybuilder()
    .size(50000)
    .filter("term", "dashboard_id", dashboardID)
    .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  return results["hits"]["hits"].reduce(
    (resultsMap, record) => ({
      ...resultsMap,
      [record["_source"]["cell_id"]]: record["_source"][label["label"]]
    }),
    {}
  );
}

async function getGeneValue(dashboardID, label) {
  const query = bodybuilder()
    .size(50000)
    .filter("term", "gene", label["label"])
    .build();

  const results = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: query
  });

  return results["hits"]["hits"].reduce(
    (resultsMap, record) => ({
      ...resultsMap,
      [record["_source"]["cell_id"]]: record["_source"]["log_count"]
    }),
    {}
  );
}

async function getCelltypeValue(dashboardID, label) {
  const query = bodybuilder()
    .size(50000)
    .filter("term", "dashboard_id", dashboardID)
    .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  return results["hits"]["hits"].reduce(
    (resultsMap, record) => ({
      ...resultsMap,
      [record["_source"]["cell_id"]]: record["_source"]["cell_type"]
    }),
    {}
  );
}

async function getSampleValue(dashboardID, label) {
  const sampleMap = getSampleMap(dashboardID, label["label"]);

  const query = bodybuilder()
    .size(50000)
    .filter("term", "dashboard_id", dashboardID)
    .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  return results["hits"]["hits"].reduce(
    (resultsMap, record) => ({
      ...resultsMap,
      [record["_source"]["cell_id"]]: sampleMap[record["_source"]["sample_id"]]
    }),
    {}
  );
}
