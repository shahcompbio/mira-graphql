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
  }

  type Dashboard {
    id: ID
    samples: [Sample!]!
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
        .agg("terms", "patient_id")
        .agg("terms", "surgery")
        .agg("terms", "site")
        .agg("terms", "sort");

      const query = addFilters(baseQuery, filters).build();

      const results = await client.search({
        index: "sample_metadata",
        body: query
      });

      return { type, results, filters };
    }
  },

  DashboardCluster: {
    dashboards: async root => {
      const { type, results } = root;

      // This only works for sample level. Gonna have to figure out a smart solution for patient/site/whatev

      if (type === "sample") {
        return results["hits"]["hits"]
          .map(record => ({
            type: root,
            ...record["_source"]
          }))
          .sort((a, b) => (a["sample_id"] > b["sample_id"] ? 1 : -1));
      }

      // const baseQuery = bodybuilder().size(10000);
      // //.filter("term", "type", root["type"]); / Not needed for sample level

      // const query = addFilters(baseQuery, root["filters"]).build();

      // const results = await client.search({
      //   index: "sample_metadata",
      //   body: query
      // });

      // return results["hits"]["hits"]
      //   .map(record => ({
      //     type: root,
      //     ...record["_source"]
      //   }))
      //   .sort((a, b) => (a["dashboard_id"] > b["dashboard_id"] ? 1 : -1));
    },

    metadata: async root => {
      // TODO: Flesh this out. Right now we can (safely) assume just want to scrape for all possible values

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
    }
  },

  Dashboard: {
    id: root => root["sample_id"],
    samples: async root => {
      return [root];
      // const sampleIDs = await getSampleIDs(root["type"], root["sample_id"]);

      // const query = bodybuilder()
      //   .size(10000)
      //   .filter("terms", "sample_id", sampleIDs)
      //   .build();

      // const results = await client.search({
      //   index: "sample_metadata",
      //   body: query
      // });

      // return results["hits"]["hits"]
      //   .map(record => record["_source"])
      //   .sort((a, b) => (a["sample_id"] > b["sample_id"] ? 1 : -1));
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
      })),
    stats: async root => {
      const sampleID = root["sample_id"];

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
            id: `${root["sample_id"]}_${stat}_${value}`,
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
