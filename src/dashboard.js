const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    dashboardTypes: [String!]!
    dashboardClusters(type: String!, filters: [filterInput]!): DashboardCluster!
    dashboards: [Dashboard2!]!
  }

  type Dashboard2 {
    type: String!
    id: String!
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
    async dashboards() {
      const query = bodybuilder().size(50000).sort("dashboard_id").build();

      const results = await client.search({
        index: "dashboard_entry",
        body: query,
      });

      return results["hits"]["hits"].map((record) => ({
        type: record["_source"]["type"],
        id: record["_source"]["dashboard_id"],
      }));
    },

    async dashboardTypes() {
      // const query = bodybuilder()
      //   .size(0)
      //   .agg("terms", "type", { size: 50 })
      //   .build();

      // const results = await client.search({
      //   index: "dashboard_entry",
      //   body: query
      // });

      // return results["aggregations"]["agg_terms_type"]["buckets"].map(
      //   element => element.key
      // );

      return ["patient", "cohort"];
    },

    async dashboardClusters(_, { type, filters }) {
      const baseQuery = bodybuilder()
        .size(10000)
        .filter("term", "type", type)
        .aggregation("nested", { path: "samples" }, "agg_samples", (a) =>
          a
            .agg("terms", "samples.patient_id", {
              size: 200,
              order: {
                _term: "asc",
              },
            })
            .agg("terms", "samples.surgery", {
              size: 200,
              order: {
                _term: "asc",
              },
            })
            .agg("terms", "samples.site", {
              size: 200,
              order: {
                _term: "asc",
              },
            })
            .agg("terms", "samples.sort", {
              size: 200,
              order: {
                _term: "asc",
              },
            })
        );

      const query = hasFilter(filters)
        ? baseQuery.query(
            "nested",
            "path",
            "samples",
            addSampleFilters(filters)
          )
        : baseQuery;

      const results = await client.search({
        index: "dashboard_entry",
        body: query.build(),
      });

      return { results, filters };
    },
  },

  DashboardCluster: {
    dashboards: async (root) => {
      const { results } = root;

      return results["hits"]["hits"]
        .map((record) => record["_source"])
        .sort((a, b) => (a["dashboard_id"] > b["dashboard_id"] ? 1 : -1));
    },

    metadata: async (root) => {
      const { results, filters } = root;

      return ["patient_id", "surgery", "site", "sort"].map((option) => ({
        id: `metadata_${option}_${filters.reduce(
          (id, filter) => (filter ? `${id}_${filter["value"]}` : `${id}_null`),
          ""
        )}`,
        name: {
          patient_id: "Patient",
          surgery: "Surgery",
          site: "Site",
          sort: "Sort",
        }[option],
        key: option,
        values: results["aggregations"]["agg_samples"][
          `agg_terms_samples.${option}`
        ]["buckets"]
          .map((bucket) => bucket["key"])
          .sort(),
      }));
    },

    stats: (root) => {
      // TODO: Unhardcode this. but right now it's the same across all samples. No need to change a good thing.
      return [];
    },
  },

  Dashboard: {
    id: (root) => root["dashboard_id"],
    samples: (root) =>
      root.samples.sort((a, b) => (a["sample_id"] < b["sample_id"] ? -1 : 1)),
    metadata: (root) =>
      ["patient_id", "surgery", "site", "sort"].map((option) => ({
        id: `${root["dashboard_id"]}_${option}`,
        name: {
          patient_id: "Patient",
          surgery: "Surgery",
          site: "Site",
          sort: "Sort",
        }[option],
        key: option,
        values: [
          ...new Set(root.samples.map((sample) => sample[option])),
        ].sort(),
      })),
  },

  Sample: {
    id: (root) => root["sample_id"],
    name: (root) => root["sample_id"],
    metadata: (root) =>
      ["patient_id", "surgery", "site", "sort"].map((option) => ({
        id: `${root["sample_id"]}_${option}`,
        name: {
          patient_id: "Patient",
          surgery: "Surgery",
          site: "Site",
          sort: "Sort",
        }[option],
        value: root[option],
      })),
    stats: async (root) => [],
  },
};

const hasFilter = (filters) =>
  filters.reduce((answer, filter) => answer || filter !== null, false);
const addSampleFilters = (filters) => (query) =>
  filters.reduce(
    (oldQuery, filter) =>
      filter
        ? oldQuery.query("match", `samples.${filter["key"]}`, filter["value"])
        : oldQuery,
    query
  );
