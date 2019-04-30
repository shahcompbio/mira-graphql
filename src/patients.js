const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    patients: [String!]!
    samples(patientID: String!): [String!]!
  }
`;

export const resolvers = {
  Query: {
    async patients() {
      const query = bodybuilder()
        .size(50000)
        .aggregation("terms", "patient_id")
        .build();

      const results = await client.search({
        index: "patient_metadata",
        body: query
      });

      return results["aggregations"]["agg_terms_patient_id"]["buckets"].map(
        bucket => bucket.key
      );
    },

    async samples(_, { patientID }) {
      const query = bodybuilder()
        .size(50000)
        .aggregation("terms", "sample_id")
        .filter("term", "patient_id", patientID)
        .build();

      const results = await client.search({
        index: "patient_metadata",
        body: query
      });
      return results["aggregations"]["agg_terms_sample_id"]["buckets"].map(
        bucket => bucket.key
      );
    }
  }
};
