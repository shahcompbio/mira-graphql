const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import getSampleIDs from "./utils/getSampleIDs.js";

export const schema = gql`
  extend type Query {
    dashboardTypes: [String!]!
    dashboardClusters(type: String!, filters: [filterInput]!): DashboardCluster!
  }

  type DashboardCluster {
    dashboards: [Dashboard!]!
    metadata: [Option!]!
    stats: [String!]!
  }

  type Dashboard {
    id: ID
    samples: [Sample!]!
    metadata: [Option!]!
  }

  type Sample {
    id: ID
    name: String!
    metadata: [Metadatum!]!
    stats: [Metadatum!]!
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

  input filterInput {
    key: String!
    value: String!
  }
`;

export const resolvers = {
  Query: {
    async dashboardTypes() {
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
    },

    async dashboardClusters(_, { type, filters }) {
      const baseQuery = bodybuilder()
        .size(10000)
        .filter("term", "type", type)
        .agg("terms", "patient_id")
        .agg("terms", "surgery")
        .agg("terms", "site")
        .agg("terms", "sort");

      const query = addFilters(baseQuery, filters).build();

      const results = await client.search({
        index: "dashboard_entry",
        body: query
      });

      return { results, filters };
    }
  },

  DashboardCluster: {
    dashboards: async root => {
      const { results } = root;

      return results["hits"]["hits"]
        .map(record => record["_source"])
        .sort((a, b) => (a["dashboard_id"] > b["dashboard_id"] ? 1 : -1));
    },

    metadata: async root => {
      const { results, filters } = root;

      return ["patient_id", "surgery", "site", "sort"].map(option => ({
        id: `metadata_${option}_${filters.reduce(
          (id, filter) => (filter ? `${id}_${filter["value"]}` : `${id}_null`),
          ""
        )}`,
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
    },

    stats: root => {
      // TODO: Unhardcode this. but right now it's the same across all samples. No need to change a good thing.
      return [
        "Estimated Number of Cells",
        "Median Genes per Cell",
        "Median UMI Counts",
        "Mito20",
        "Number of Genes",
        "Number of Reads"
      ];
    }
  },

  Dashboard: {
    id: root => root["dashboard_id"],
    samples: async root => {
      if (root["type"] === "sample") {
        return [root];
      } else {
        const sampleIDs = root["sample_ids"];
        const query = bodybuilder()
          .size(1000)
          .filter("terms", "dashboard_id", sampleIDs)
          .build();

        const results = await client.search({
          index: "dashboard_entry",
          body: query
        });
        return results["hits"]["hits"]
          .map(record => record["_source"])
          .sort((a, b) => (a["dashboard_id"] < b["dashboard_id"] ? -1 : 1));
      }
    },
    metadata: root =>
      ["patient_id", "surgery", "site", "sort"].map(option => ({
        id: `${root["dashboard_id"]}_${option}`,
        name: {
          patient_id: "Patient",
          surgery: "Surgery",
          site: "Site",
          sort: "Sort"
        }[option],
        key: option,
        values: Array.isArray(root[option]) ? root[option] : [root[option]]
      }))
  },

  Sample: {
    id: root => root["dashboard_id"],
    name: root => root["dashboard_id"],
    metadata: root =>
      ["patient_id", "surgery", "site", "sort"].map(option => ({
        id: `${root["dashboard_id"]}_${option}`,
        name: {
          patient_id: "Patient",
          surgery: "Surgery",
          site: "Site",
          sort: "Sort"
        }[option],
        value: Array.isArray(root[option]) ? root[option][0] : root[option]
      })),
    stats: async root => {
      const sampleID = root["dashboard_id"];

      const query = bodybuilder()
        .size(10000)
        .filter("term", "sample_id", sampleID)
        .build();

      const results = await client.search({
        index: "sample_stats",
        body: query
      });

      return results["hits"]["hits"]
        .map(record => {
          const data = record["_source"];
          const { stat, value } = data;
          return {
            id: `${sampleID}_${stat}_${value}`,
            name: stat,
            value
          };
        })
        .sort((a, b) => (a["name"] <= b["name"] ? -1 : 1));
    }
  }
};

const addFilters = (query, filters) =>
  filters.reduce(
    (oldQuery, filter) =>
      filter
        ? oldQuery.filter("term", filter["key"], filter["value"])
        : oldQuery,
    query
  );
