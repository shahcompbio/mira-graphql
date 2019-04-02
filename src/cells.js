const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cells(sampleID: String!): [Cell!]!
    dimensionRanges(sampleID: String!): Axis!
    clusters(sampleID: String!): [Cluster!]!
    celltypes(sampleID: String!): [Celltype!]!
  }

  type Cell {
    id: String!
    x: Float!
    y: Float!
    cluster: String!
    celltype: String!
  }

  type Axis {
    x: [Float!]!
    y: [Float!]!
  }

  type Cluster {
    id: String!
    count: Int!
  }

  type Celltype {
    id: String!
    count: Int!
  }
`;

export const resolvers = {
  Query: {
    async cells(_, { sampleID }) {
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

    async dimensionRanges(_, { sampleID }) {
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

    async clusters(_, { sampleID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("terms", "cluster", { order: { _key: "asc" } })
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });

      return results["aggregations"]["agg_terms_cluster"]["buckets"];
    },

    async celltypes(_, { sampleID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("terms", "cell_type", { order: { _key: "asc" } })
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });

      return results["aggregations"]["agg_terms_cell_type"]["buckets"];
    }
  },

  Cell: {
    id: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    cluster: root => root["cluster"].toString(),
    celltype: root => root["cell_type"]
  },

  Axis: {
    x: root => [root["agg_min_x"]["value"], root["agg_max_x"]["value"]],
    y: root => [root["agg_min_y"]["value"], root["agg_max_y"]["value"]]
  },

  Cluster: {
    id: root => root.key.toString(),
    count: root => root.doc_count
  },

  Celltype: {
    id: root => root.key,
    count: root => root.doc_count
  }
};
