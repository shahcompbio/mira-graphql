const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import getSampleIDs from "./utils/getSampleIDs.js";

export const schema = gql`
  extend type Query {
    celltypes(type: String, dashboardID: String): [Rho!]!
  }

  type Rho {
    id: String!
    name: String!
    count: Int!
    markers: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async celltypes(_, { type, dashboardID }) {
      const query = bodybuilder()
        .size(100)
        .filter("term", "dashboard_id", dashboardID)
        .build();

      const results = await client.search({
        index: "marker_genes",
        body: query,
      });

      return results["hits"]["hits"]
        .map((record) => record["_source"])
        .sort((a, b) => (a["cell_type"] < b["cell_type"] ? -1 : 1));
    },
  },

  Rho: {
    id: (root) => `rho_${root["dashboard_id"]}_${root["cell_type"]}`,
    name: (root) => root["cell_type"],
    count: (root) => root["count"],
    markers: (root) => root["genes"],
  },
};
