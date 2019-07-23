const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";
import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cellAndMarkerGenesPair(patientID: String!): [Rho!]!
    existingCellTypes(patientID: String!, sampleID: String!): [Pairs!]
  }

  type Pairs {
    cell: String
    count: Int
  }

  type Rho {
    cellType: String!
    markerGenes: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async cellAndMarkerGenesPair(_, { patientID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "patient_id", patientID)
        .aggregation("terms", "celltype", { size: 100 }, a => {
          return a.aggregation("terms", "marker_gene", { size: 100 });
        })
        .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_rho`,
        body: query
      });

      return results["aggregations"]["agg_terms_celltype"]["buckets"];
    },
    async existingCellTypes(_, { patientID, sampleID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("terms", "cell_type", { size: 100 })
        .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_cells`,
        body: query
      });

      const sorted = results["aggregations"]["agg_terms_cell_type"]["buckets"]
        .map(element => element.key)
        .sort();

      return sorted.map(
        cellType =>
          results["aggregations"]["agg_terms_cell_type"]["buckets"].filter(
            element => element.key === cellType
          )[0]
      );
    }
  },
  Pairs: {
    cell: root => root.key,
    count: root => root["doc_count"]
  },

  Rho: {
    cellType: root => root.key,
    markerGenes: root =>
      root["agg_terms_marker_gene"]["buckets"].map(marker => marker.key)
  }
};
