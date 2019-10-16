const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import getSampleIDs from "./utils/getSampleIDs";

export const schema = gql`
  extend type Query {
    sampleStats(type: String!, dashboardID: String!): [SampleStats!]!
    sampleStatsHeaders(type: String!, dashboardID: String!): [String!]!
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
    async sampleStats(_, { type, dashboardID }) {
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
    },

    async sampleStatsHeaders(_, { type, dashboardID }) {
      const sampleIDs = await getSampleIDs(type, dashboardID);
      const query = bodybuilder()
        .size(0)
        .filter("terms", "sample_id", sampleIDs)
        .agg("terms", "stat")
        .build();

      const results = await client.search({
        index: "sample_stats",
        body: query
      });

      const METADATA_HEADERS = ["patientID", "surgery", "site", "sort"];

      const STATS_HEADERS = results["aggregations"]["agg_terms_stat"]["buckets"]
        .map(bucket => bucket["key"])
        .sort();
      return [...METADATA_HEADERS, ...STATS_HEADERS];
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
