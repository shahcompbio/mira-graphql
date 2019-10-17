const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import getSampleIDs from "./utils/getSampleIDs.js";

export const schema = gql`
  extend type Query {
    dashboardClusters: [DashboardCluster!]!
  }

  type DashboardCluster {
    type: String!
    dashboards: [Dashboard!]!
    metadata: [Option!]!
  }

  type Dashboard {
    id: ID
    samples: [Sample!]!
  }

  type Sample {
    id: ID
    name: String!
    metadata: [Metadatum!]!
  }

  type Metadatum {
    id: ID
    name: String!
    value: String!
  }

  type Option {
    id: ID
    name: String!
    key: String!
    values: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async dashboardClusters() {
      const query = bodybuilder()
        .size(0)
        .agg("terms", "type", { size: 50 })
        .build();

      const results = await client.search({
        index: "dashboard_entry",
        body: query
      });

      return results["aggregations"]["agg_terms_type"]["buckets"].map(
        element => element.key
      );
    }
  },

  DashboardCluster: {
    type: root => root,
    dashboards: async root => {
      const query = bodybuilder()
        .size(10000)
        .filter("term", "type", root)
        .build();

      const results = await client.search({
        index: "dashboard_entry",
        body: query
      });

      return results["hits"]["hits"]
        .map(record => ({
          type: root,
          ...record["_source"]
        }))
        .sort((a, b) => (a["dashboard_id"] > b["dashboard_id"] ? 1 : -1));
    },

    metadata: async root => {
      // TODO: Flesh this out. Right now we can (safely) assume just want to scrape for all possible values

      const query = bodybuilder()
        .size(0)
        .agg("terms", "patient_id")
        .agg("terms", "surgery")
        .agg("terms", "site")
        .agg("terms", "sort")
        .build();

      const results = await client.search({
        index: "sample_metadata",
        body: query
      });

      return ["patient_id", "surgery", "site", "sort"].map(option => ({
        id: `${root}_${option}`,
        name: {
          patient_id: "Patient",
          surgery: "Surgery",
          site: "Site",
          sort: "Sort"
        }[option],
        key: option,
        values: results["aggregations"][`agg_terms_${option}`]["buckets"]
          .map(bucket => bucket["key"])
          .sort()
      }));
    }
  },

  Dashboard: {
    id: root => root["dashboard_id"],
    samples: async root => {
      const sampleIDs = await getSampleIDs(root["type"], root["dashboard_id"]);

      const query = bodybuilder()
        .size(10000)
        .filter("terms", "sample_id", sampleIDs)
        .build();

      const results = await client.search({
        index: "sample_metadata",
        body: query
      });

      return results["hits"]["hits"]
        .map(record => record["_source"])
        .sort((a, b) => (a["sample_id"] > b["sample_id"] ? 1 : -1));
    }
  },

  Sample: {
    id: root => root["sample_id"],
    name: root => root["sample_id"],
    metadata: root =>
      ["patient_id", "surgery", "site", "sort"].map(option => ({
        id: `${root["sample_id"]}_${option}`,
        name: {
          patient_id: "Patient",
          surgery: "Surgery",
          site: "Site",
          sort: "Sort"
        }[option],
        value: root[option]
      }))
  }
};
