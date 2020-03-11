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
    count: Int!
  }
`;

const NUM_BINS = 50;

export const resolvers = {
  Query: {
    async correlation(_, { dashboardID, labels }) {
      // Here we assume there's only two labels
      const [xLabel, yLabel] = labels;

      const cellIDs = await getCellIDs(dashboardID);
      const [xData, xBins, xBinSize] = await getData(dashboardID, xLabel);
      const [yData, yBins, yBinSize] = await getData(dashboardID, yLabel);

      const yGrid = yBins.reduce(
        (currGrid, yBin) => ({ ...currGrid, [yBin]: 0 }),
        {}
      );
      let grid = xBins.reduce(
        (currGrid, xBin) => ({ ...currGrid, [xBin]: { ...yGrid } }),
        {}
      );

      for (const cellID of cellIDs) {
        // need to account for numerical bins
        let x = xData.hasOwnProperty(cellID)
          ? getBinKey(xData, cellID, xBinSize)
          : 0;
        let y = yData.hasOwnProperty(cellID)
          ? getBinKey(yData, cellID, yBinSize)
          : 0;

        grid[x][y] = grid[x][y] + 1;
      }

      const getRecord = (x, y, count) => ({
        x: xBinSize === 0 ? x : x * xBinSize,
        y: yBinSize === 0 ? y : y * yBinSize,
        count
      });

      const getYRecords = xBin => {
        const records = yBins.reduce(
          (currRecords, yBin) => [
            ...currRecords,
            getRecord(xBin, yBin, grid[xBin][yBin])
          ],
          []
        );
        return records;
      };

      const records = xBins.reduce(
        (currRecords, xBin) => [...currRecords, ...getYRecords(xBin)],
        []
      );

      return records;
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

const getBinKey = (data, cellID, binSize) =>
  binSize === 0
    ? data[cellID]
    : Math.min(NUM_BINS - 1, Math.floor(data[cellID] / binSize));

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

  return [
    results["hits"]["hits"].reduce(
      (resultsMap, record) => ({
        ...resultsMap,
        [record["_source"]["cell_id"]]: record["_source"][label["label"]]
      }),
      {}
    ),
    [...Array(NUM_BINS)].map((_, i) => i),
    1 / NUM_BINS
  ];
}

async function getGeneValue(dashboardID, label) {
  const query = bodybuilder()
    .size(50000)
    .filter("term", "gene", label["label"])
    .agg("stats", "log_count")
    .build();

  const results = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: query
  });

  return [
    results["hits"]["hits"].reduce(
      (resultsMap, record) => ({
        ...resultsMap,
        [record["_source"]["cell_id"]]: record["_source"]["log_count"]
      }),
      {}
    ),
    [...Array(NUM_BINS)].map((_, i) => i),
    (results["aggregations"]["agg_stats_log_count"]["max"] -
      results["aggregations"]["agg_stats_log_count"]["min"]) /
      NUM_BINS
  ];
}

async function getCelltypeValue(dashboardID, label) {
  const query = bodybuilder()
    .size(50000)
    .filter("term", "dashboard_id", dashboardID)
    .agg("terms", "cell_type", { size: 1000 })
    .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  return [
    results["hits"]["hits"].reduce(
      (resultsMap, record) => ({
        ...resultsMap,
        [record["_source"]["cell_id"]]: record["_source"]["cell_type"]
      }),
      {}
    ),
    [
      ...results["aggregations"]["agg_terms_cell_type"]["buckets"]
        .map(bucket => bucket["key"])
        .sort(),
      "Other"
    ],
    0
  ];
}

async function getSampleValue(dashboardID, label) {
  const sampleMap = await getSampleMap(dashboardID, label["label"]);

  const query = bodybuilder()
    .size(50000)
    .filter("term", "dashboard_id", dashboardID)
    .agg("terms", "sample_id", { size: 1000 })
    .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  return [
    results["hits"]["hits"].reduce(
      (resultsMap, record) => ({
        ...resultsMap,
        [record["_source"]["cell_id"]]:
          sampleMap[record["_source"]["sample_id"]]
      }),
      {}
    ),
    results["aggregations"]["agg_terms_sample_id"]["buckets"].reduce(
      (mapping, bucket) =>
        mapping.indexOf(sampleMap[bucket["key"]]) === -1
          ? [...mapping, sampleMap[bucket["key"]]]
          : mapping,
      []
    ),
    0
  ];
}
