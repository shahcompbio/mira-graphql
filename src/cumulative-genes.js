const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cumulativeGenes(dashboardID: String!, genes: [String!]!): [DensityBin!]!
    verifyGenes(dashboardID: String!, genes: [String!]!): GeneList!
  }

  type GeneList {
    valid: [String!]!
    invalid: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async verifyGenes(_, { dashboardID, genes }) {
      if (genes.length === 0) {
        return { valid: [], invalid: [] };
      }
      const query = bodybuilder()
        .size(0)
        .filter("terms", "gene", genes)
        .agg("terms", "gene", { size: 10000 })
        .build();

      const results = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: query
      });

      const validGenes = results["aggregations"]["agg_terms_gene"][
        "buckets"
      ].map(bucket => bucket["key"]);

      return {
        valid: genes.filter(gene => validGenes.indexOf(gene) !== -1),
        invalid: genes.filter(gene => validGenes.indexOf(gene) === -1)
      };
    },

    async cumulativeGenes(_, { dashboardID, genes }) {
      const sizeQuery = bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("stats", "x")
        .aggregation("stats", "y")
        .build();

      const sizeResults = await client.search({
        index: "dashboard_cells",
        body: sizeQuery
      });

      const { agg_stats_x, agg_stats_y } = sizeResults["aggregations"];

      const xBinSize = (agg_stats_x["max"] - agg_stats_x["min"]) / 100;
      const yBinSize = (agg_stats_y["max"] - agg_stats_y["min"]) / 100;

      const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);

      const dataBins = await getData(dashboardID, xBinSize, yBinSize, genes);
      return allBins.map(record => ({
        ...record,
        label: "",
        value:
          dataBins.hasOwnProperty(record["x"]) &&
          dataBins[record["x"]].hasOwnProperty(record["y"])
            ? dataBins[record["x"]][record["y"]]
            : 0
      }));
    }
  }
};
async function getAllBins(dashboardID, xBinSize, yBinSize) {
  const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
    a.aggregation("terms", "cell_type", { size: 1000 })
  );
  const results = await client.search({
    index: "dashboard_cells",
    body: query.build()
  });

  return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
    (records, xBucket) => [
      ...records,
      ...processXBuckets(
        xBucket,
        xBinSize,
        yBinSize,
        "total",
        yBucket => yBucket["doc_count"]
      )
    ],
    []
  );
}

const getBaseDensityQuery = (dashboardID, xBinSize, yBinSize, labelAgg) =>
  bodybuilder()
    .size(0)
    .filter("term", "dashboard_id", dashboardID)
    .aggregation(
      "histogram",
      "x",
      { interval: xBinSize, min_doc_count: 1 },
      a =>
        a.aggregation(
          "histogram",
          "y",
          { interval: yBinSize, min_doc_count: 1 },
          labelAgg
        )
    );

const processXBuckets = (xBucket, xBinSize, yBinSize, label, getValue) =>
  xBucket["agg_histogram_y"]["buckets"].map(yBucket => {
    return {
      x: Math.round(xBucket["key"] / xBinSize),
      y: Math.round(yBucket["key"] / yBinSize),
      value: getValue(yBucket),
      label
    };
  });

async function getData(dashboardID, xBinSize, yBinSize, genes) {
  if (genes.length === 0) {
    return {};
  }

  const getDataBins = (results, getValue) =>
    results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
      (xMap, xBucket) => ({
        ...xMap,
        [Math.round(xBucket["key"] / xBinSize)]: processProbYBucket(
          xBucket["agg_histogram_y"]["buckets"],
          getValue
        )
      }),
      {}
    );

  const processProbYBucket = (yBuckets, getValue) =>
    yBuckets.reduce(
      (yMap, bucket) => ({
        ...yMap,
        [Math.round(bucket["key"] / yBinSize)]: getValue(bucket)
      }),
      {}
    );
  const query = bodybuilder()
    .size(0)
    .filter("terms", "gene", genes)
    .aggregation(
      "histogram",
      "x",
      { interval: xBinSize, min_doc_count: 1 },
      a =>
        a.aggregation(
          "histogram",
          "y",
          { interval: yBinSize, min_doc_count: 1 },
          a => a.aggregation("stats", "log_count")
        )
    )
    .build();

  const results = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: query
  });

  const dataBins = getDataBins(
    results,
    yBucket => yBucket["agg_stats_log_count"]["sum"]
  );
  return dataBins;
}
