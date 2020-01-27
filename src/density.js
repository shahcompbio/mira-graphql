const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    density(type: String!, dashboardID: String!): [Bins!]
    density2(type: String!, dashboardID: String!): [DensityBin!]
    density3(
      type: String!
      dashboardID: String!
      highlightedGroup: String
    ): [DensityBin2!]
  }
  type Bins {
    size: Int!
    bin: [DensityBin!]
  }

  type DensityBin {
    x: Float!
    y: Float!
    values: [DensityBinValue!]
  }

  type DensityBinValue {
    celltype: String!
    proportion: Float!
    count: Int!
  }

  type DensityBin2 {
    x: Float!
    y: Float!
    values: [DensityBinValue2!]
  }

  type DensityBinValue2 {
    label: String!
    value: Float!
  }
`;

export const resolvers = {
  Query: {
    async density(_, { type, dashboardID }) {
      const query = bodybuilder()
        .size(50000)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("terms", "bin_size", {
          size: 50000,
          order: { _key: "asc" }
        })
        .build();

      const results = await client.search({
        index: `dashboard_cells_density`,
        body: query
      });
      const records = results["aggregations"]["agg_terms_bin_size"][
        "buckets"
      ].map(bucket => ({
        binSize: bucket["key"],
        dashboardID
      }));
      return records;
    },

    async density2(_, { type, dashboardID }) {
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

      const query = bodybuilder()
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
              a => a.aggregation("terms", "cell_type", { size: 1000 })
            )
        )
        .build();

      const results = await client.search({
        index: "dashboard_cells",
        body: query
      });

      const records = results["aggregations"]["agg_histogram_x"][
        "buckets"
      ].reduce(
        (records, xBucket) => [...records, ...processXBuckets(xBucket)],
        []
      );

      return records;
    },

    async density3(_, { type, dashboardID, highlightedGroup }) {
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

      const query = bodybuilder()
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
              a => a.aggregation("terms", "cell_type", { size: 1000 })
            )
        )
        .build();

      const results = await client.search({
        index: "dashboard_cells",
        body: query
      });

      const records = results["aggregations"]["agg_histogram_x"][
        "buckets"
      ].reduce(
        (records, xBucket) => [
          ...records,
          ...processXBuckets2(xBucket, highlightedGroup)
        ],
        []
      );

      return records;
    }
  },

  Bins: {
    size: root => root["binSize"],
    bin: async ({ binSize, dashboardID }) => {
      const query = bodybuilder()
        .size(50000)
        .filter("term", "dashboard_id", dashboardID)
        .filter("term", "bin_size", binSize)
        .sort([{ x: "asc" }, { y: "asc" }, { count: "desc" }])
        .build();

      const results = await client.search({
        index: `dashboard_cells_density`,
        body: query
      });
      const records = condenseResults(
        results["hits"]["hits"].map(record => record["_source"])
      );
      return records;
    }
  },

  DensityBin: {
    x: root => root["x"],
    y: root => root["y"],
    values: root => {
      const total = root["values"].reduce(
        (total, record) => total + record["count"],
        0
      );

      return root["values"].map(record => ({ ...record, total }));
    }
  },

  DensityBin2: {
    x: root => root["x"],
    y: root => root["y"],
    values: root => {
      const total = root["values"].reduce(
        (total, record) => total + record["count"],
        0
      );

      return root["values"].map(record => ({
        ...record,
        total,
        highlightedGroup: root["highlightedGroup"]
      }));
    }
  },

  DensityBinValue: {
    celltype: root => root["celltype"],
    proportion: root => root["count"] / root["total"],
    count: root => root["count"]
  },

  DensityBinValue2: {
    label: root => root["celltype"],
    value: root =>
      !root["highlightedGroup"] ? root["count"] : root["count"] / root["total"]
  }
};

const condenseResults = records => {
  const helper = (records, rsf, currXY, acc) => {
    if (records.length === 0) {
      return [...rsf, { ...currXY, values: acc }];
    } else {
      const [currRecord, ...restRecords] = records;
      if (currXY["x"] === currRecord["x"] && currXY["y"] === currRecord["y"]) {
        return helper(restRecords, rsf, currXY, [...acc, currRecord]);
      } else {
        return helper(
          restRecords,
          [...rsf, { ...currXY, values: acc }],
          currRecord,
          [currRecord]
        );
      }
    }
  };
  const [firstRecord, ...restRecords] = records;
  return helper(restRecords, [], firstRecord, [firstRecord]);
};

const processXBuckets = xBucket =>
  xBucket["agg_histogram_y"]["buckets"].map(yBucket => ({
    x: xBucket["key"],
    y: yBucket["key"],
    values: yBucket["agg_terms_cell_type"]["buckets"].map(celltypeBucket => ({
      celltype: celltypeBucket["key"],
      count: celltypeBucket["doc_count"]
    }))
  }));

const processXBuckets2 = (xBucket, highlightedGroup) =>
  xBucket["agg_histogram_y"]["buckets"].map(yBucket => ({
    x: xBucket["key"],
    y: yBucket["key"],
    highlightedGroup,
    values: yBucket["agg_terms_cell_type"]["buckets"].map(celltypeBucket => ({
      celltype: celltypeBucket["key"],
      count: celltypeBucket["doc_count"]
    }))
  }));
