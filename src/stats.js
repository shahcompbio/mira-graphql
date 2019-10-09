const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import getSampleIDs from "./utils/getSampleIDs";

export const schema = gql`
  extend type Query {
    sample_stats(type: String!, dashboardID: String!): [SampleStats!]!
  }

  type SampleStats {
    patientID: String!
    sampleID: String!
    site: String!
    surgery: String!
    sort: String!
    values: [Stat!]!
  }

  type Stat {
    id: ID
    name: String!
    value: Int!
  }
`;

export const resolvers = {
  Query: {
    async sample_stats(_, { type, dashboardID }) {
      const sampleIDs = await getSampleIDs(type, dashboardID);

      const query = bodybuilder()
        .size(10000)
        .filter("terms", "sample_id", sampleIDs)
        .build();

      const results = await client.search({
        index: "sample_metadata",
        body: query
      });

      return results["hits"]["hits"].map(record => record["_source"]);
    }
  },

  SampleStats: {
    patientID: root => root["patient_id"],
    sampleID: root => root["sample_id"],
    site: root => root["site"],
    surgery: root => root["surgery"],
    sort: root => root["sort"],
    values: async root => {
      const sampleID = root["sample_id"];

      const query = bodybuilder()
        .size(10000)
        .filter("term", "sample_id", sampleID)
        .build();

      const results = await client.search({
        index: "sample_stats",
        body: query
      });

      return results["hits"]["hits"].map(record => record["_source"]);
    }
  },

  Stat: {
    id: root => `${root["sample_id"]}_${root["stat"]}`,
    name: root => root["stat"],
    value: root => root["value"]
  }
};
