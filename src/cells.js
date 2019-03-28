const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    getRedDim(sampleID: String!): [Cell!]!
    getRedDimRanges(sampleID: String!): Axis!
    getClusters(sampleID: String!): [Int!]!
    getCelltypes(sampleID: String!): [String!]!
  }

  type Cell {
    id: String!
    x: Float!
    y: Float!
    cluster: Int!
    celltype: String!
  }

  type Axis {
    x: [Float!]!
    y: [Float!]!
  }
`;

export const resolvers = {
  Query: {
    async getRedDim(_, { sampleID }) {
      const query = bodybuilder()
        .size(50000)
        .filter("term", "sample_id", sampleID)
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });
      return results.hits.hits.map(hit => hit["_source"]);
    },

    async getRedDimRanges(_, { sampleID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("min", "x")
        .aggregation("max", "x")
        .aggregation("min", "y")
        .aggregation("max", "y")
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });

      return results["aggregations"];
    },

    async getClusters(_, { sampleID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("terms", "cluster", { order: { _key: "asc" } })
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });

      return results["aggregations"]["agg_terms_cluster"]["buckets"].map(
        bucket => bucket.key
      );
    },

    async getCelltypes(_, { sampleID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("terms", "cell_type", { order: { _key: "asc" } })
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });

      return results["aggregations"]["agg_terms_cell_type"]["buckets"].map(
        bucket => bucket.key
      );
    }
  },

  Cell: {
    id: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    cluster: root => root["cluster"],
    celltype: root => root["cell_type"]
  },

  Axis: {
    x: root => [root["agg_min_x"]["value"], root["agg_max_x"]["value"]],
    y: root => [root["agg_min_y"]["value"], root["agg_max_y"]["value"]]
  }
};
