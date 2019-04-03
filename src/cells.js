const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import { GraphQLScalarType, Kind } from "graphql";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cells(sampleID: String!, label: String!): [Cell!]!
    dimensionRanges(sampleID: String!): Axis!
  }

  scalar StringOrNum

  type Cell {
    id: String!
    x: Float!
    y: Float!
    label: StringOrNum!
  }

  type Axis {
    x: [Float!]!
    y: [Float!]!
  }
`;

export const resolvers = {
  Query: {
    async cells(_, { sampleID, label }) {
      const query = bodybuilder()
        .size(50000)
        .filter("term", "sample_id", sampleID)
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });
      return results.hits.hits.map(hit => ({
        ...hit["_source"],
        label: hit["_source"][label]
      }));
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
    }
  },
  StringOrNum: new GraphQLScalarType({
    name: "StringOrNum",
    description: "A String or a Num union type",
    serialize(value) {
      if (typeof value !== "string" && typeof value !== "number") {
        throw new Error("Value must be either a String or a Number");
      }
      return value;
    },
    parseValue(value) {
      if (typeof value !== "string" && typeof value !== "number") {
        throw new Error("Value must be either a String or an Int");
      }

      return value;
    },
    parseLiteral(ast) {
      switch (ast.kind) {
        case Kind.FIELD:
          return parseFloat(ast.value);
        case Kind.INT:
          return parseInt(ast.value, 10);
        case Kind.STRING:
          return ast.value;
        default:
          throw new Error("Value must be either a String or a Number");
      }
    }
  }),
  Cell: {
    id: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    label: root => root["label"].toString()
  },

  Axis: {
    x: root => [root["agg_min_x"]["value"], root["agg_max_x"]["value"]],
    y: root => [root["agg_min_y"]["value"], root["agg_max_y"]["value"]]
  }
};
