const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cells(sampleID: String!, label: String!, labelType: String!): [Cell!]!
  }

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
    async cells(_, { sampleID, label, labelType }) {
      const query = bodybuilder()
        .size(50000)
        .filter("term", "sample_id", sampleID)
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });

      if (labelType === "gene") {
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
  Cell: {
    id: root => `${root["sample_id"]}_${root["cell_id"]}`,
    name: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    label: root => root["label"]
  }
};
