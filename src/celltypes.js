const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";
import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cellAndMarkerGenesPair: [Rho!]!
  }

  type Rho {
    cellType: String!
    markerGenes: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async cellAndMarkerGenesPair() {
      const query = bodybuilder()
        .size(0)
        .aggregation("terms", "celltype", { size: 100 }, a => {
          return a.aggregation("terms", "marker_gene", { size: 100 });
        })
        .build();

      const results = await client.search({
        index: "patient_09443_e_rho",
        body: query
      });

      return results["aggregations"]["agg_terms_celltype"]["buckets"];
    }
  },
  Rho: {
    cellType: root => root.key,
    markerGenes: root =>
      root["agg_terms_marker_gene"]["buckets"].map(marker => marker.key)
  }
};
