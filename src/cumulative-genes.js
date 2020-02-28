const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cumulativeGenes(
      dashboardID: String!
      genes: [String!]!
      highlightedGroup: AttributeInput
    ): [DensityBin!]!
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

    async cumulativeGenes(_, { dashboardID, genes, highlightedGroup }) {
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

      const dataBins = await getData(
        dashboardID,
        xBinSize,
        yBinSize,
        genes,
        highlightedGroup
      );
      return allBins.map(record => ({
        ...record,
        label: "",
        value:
          dataBins.hasOwnProperty(record["x"]) &&
          dataBins[record["x"]].hasOwnProperty(record["y"])
            ? dataBins[record["x"]][record["y"]]
            : highlightedGroup
            ? ""
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

async function getData(
  dashboardID,
  xBinSize,
  yBinSize,
  genes,
  highlightedGroup
) {
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

  const query = await getQuery(
    dashboardID,
    genes,
    xBinSize,
    yBinSize,
    highlightedGroup
  );

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

async function getQuery(
  dashboardID,
  genes,
  xBinSize,
  yBinSize,
  highlightedGroup
) {
  const baseQuery = bodybuilder()
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
    );

  if (!highlightedGroup) {
    return baseQuery.build();
  }

  const cellIDs = await getCellIDs(dashboardID, highlightedGroup);

  return baseQuery.filter("terms", "cell_id", cellIDs).build();
}

async function getCellIDs(dashboardID, highlightedGroup) {
  if (highlightedGroup["type"] === "CELL") {
    const baseQuery = bodybuilder()
      .size(50000)
      .filter("term", "dashboard_id", dashboardID);

    const query = highlightedGroup["isNum"]
      ? baseQuery
          .filter("range", highlightedGroup["label"], {
            gte: highlightedGroup["value"].split("-")[0].trim(),
            lt:
              parseFloat(highlightedGroup["value"].split("-")[1].trim()) === 1
                ? "1.1"
                : highlightedGroup["value"].split("-")[1].trim()
          })
          .build()
      : baseQuery
          .filter(
            "term",
            highlightedGroup["label"] === "celltype"
              ? "cell_type"
              : highlightedGroup["label"],
            highlightedGroup["value"]
          )
          .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["hits"]["hits"].map(record => record["_source"]["cell_id"]);
  } else if (highlightedGroup["type"] === "SAMPLE") {
    const sampleIDQuery = bodybuilder()
      .size(1000)
      .filter("term", "patient_id", dashboardID)
      .filter("term", highlightedGroup["label"], highlightedGroup["value"])
      .filter("term", "type", "sample")
      .build();

    const sampleIDResults = await client.search({
      index: "dashboard_entry",
      body: sampleIDQuery
    });

    const sampleIDs = sampleIDResults["hits"]["hits"].reduce(
      (sampleIDs, record) => [...sampleIDs, ...record["_source"]["sample_ids"]],
      []
    );

    const query = bodybuilder()
      .size(50000)
      .filter("term", "dashboard_id", dashboardID)
      .filter("terms", "sample_id", sampleIDs)
      .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["hits"]["hits"].map(record => record["_source"]["cell_id"]);
  } else {
    // is "GENE"
    const query = bodybuilder()
      .size(0)
      .filter("term", "gene", highlightedGroup["label"])
      .filter("range", "log_count", {
        gte: highlightedGroup["value"].split("-")[0].trim(),
        lt: highlightedGroup["value"].split("-")[1].trim()
      })
      .aggregation("terms", "cell_id", { size: 50000 })
      .build();

    const results = await client.search({
      index: `dashboard_genes_${dashboardID.toLowerCase()}`,
      body: query
    });

    return results["aggregations"]["agg_terms_cell_id"]["buckets"].map(
      bucket => bucket["key"]
    );
  }
}
