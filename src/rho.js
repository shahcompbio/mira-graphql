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
        .size(0)
        .agg("terms", "celltype", { size: 50 }, a => {
          return a.aggregation("terms", "marker", { size: 50 });
        })
        .build();

      const results = await client.search({
        index: "rho_markers",
        body: query
      });

      const processedBuckets = results["aggregations"]["agg_terms_celltype"][
        "buckets"
      ]
        .map(bucket => ({
          celltype: bucket.key,
          markers: bucket["agg_terms_marker"]["buckets"].map(
            element => element["key"]
          ),
          dashboardID
        }))
        .sort((a, b) => (a["celltype"] < b["celltype"] ? -1 : 1));

      return [
        ...processedBuckets,
        { celltype: "Other", markers: [], dashboardID }
      ];
    }
  },

  Rho: {
    id: root => `rho_${root["celltype"]}`,
    name: root => root["celltype"],
    count: async root => {
      const query = bodybuilder()
        .size(10000)
        .filter("term", "dashboard_id", root["dashboardID"])
        .filter("term", "cell_type", root["celltype"])
        .build();

      const results = await client.search({
        index: "dashboard_cells",
        body: query
      });

      return results["hits"]["total"]["value"];
    },
    markers: root => root["markers"]
  }
};
