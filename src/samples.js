const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    getAllSamples: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async getAllSamples() {
      const query = bodybuilder()
        .size(50000)
        .aggregation("terms", "sample_id")
        .build();

      const results = await client.search({
        index: "scrna_cells",
        body: query
      });
      return results["aggregations"]["agg_terms_sample_id"]["buckets"].map(
        bucket => bucket.key
      );
    }
  }
};
