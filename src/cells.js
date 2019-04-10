const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import { GraphQLScalarType, Kind } from "graphql";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cells(sampleID: String!, label: String!): [Cell!]!
  }

  scalar StringOrNum

  type Cell {
    id: ID!
    name: String!
    x: Float!
    y: Float!
    label: StringOrNum!
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

      if (label !== "cell_type" && label !== "cluster") {
        const geneQuery = bodybuilder()
          .size(50000)
          .filter("term", "sample_id", sampleID)
          .filter("term", "gene", label)
          .build();

        const geneResults = await client.search({
          index: "scrna_genes",
          body: geneQuery
        });

        const geneRecords = geneResults.hits.hits.reduce((geneMap, hit) => ({
          ...geneMap,
          [hit["_source"]["cell_id"]]: hit["_source"]["count"]
        }));
        return results.hits.hits.map(hit => ({
          ...hit["_source"],
          label: geneRecords.hasOwnProperty(hit["_source"]["cell_id"])
            ? geneRecords[hit["_source"]["cell_id"]]
            : 0
        }));
      }
      return results.hits.hits.map(hit => ({
        ...hit["_source"],
        label: hit["_source"][label]
      }));
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
    id: root => `${root["sample_id"]}_${root["cell_id"]}`,
    name: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    label: root => root["label"].toString()
  }
};
