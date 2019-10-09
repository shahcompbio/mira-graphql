const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import getSampleIDs from "./utils/getSampleIDs.js";

export const schema = gql`
  extend type Query {
    dashboards: [DashboardGroup!]!
  }

  type DashboardGroup {
    type: String
    dashboards: [Dashboard!]!
  }

  type Dashboard {
    id: ID
    samples: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async dashboards() {
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

  DashboardGroup: {
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

      return results["hits"]["hits"].map(record => ({
        type: root,
        ...record["_source"]
      }));
    }
  },

  Dashboard: {
    id: root => root["dashboard_id"],
    samples: async root =>
      await getSampleIDs(root["type"], root["dashboard_id"])
  }
};
