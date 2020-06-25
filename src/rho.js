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
        .agg("terms", "cell_type", { size: 50 }, (a) => {
          return a.aggregation("terms", "gene", { size: 50 });
        })
        .build();

      const results = await client.search({
        index: "marker_genes",
        body: query,
      });
      const processedBuckets = results["aggregations"]["agg_terms_cell_type"][
        "buckets"
      ]
        .map((bucket) => ({
          cell_type: bucket.key,
          markers: bucket["agg_terms_gene"]["buckets"].map(
            (element) => element["key"]
          ),
          dashboardID,
        }))
        .sort((a, b) => (a["cell_type"] < b["cell_type"] ? -1 : 1));

      return [
        ...processedBuckets,
        { cell_type: "Other", markers: [], dashboardID },
      ];
    },
  },

  Rho: {
    id: (root) => `rho_${root["cell_type"]}`,
    name: (root) => root["cell_type"],
    count: async (root) => {
      const query = bodybuilder()
        .size(50000)
        .filter("term", "dashboard_id", root["dashboardID"])
        .filter("term", "cell_type", root["cell_type"])
        .build();

      const results = await client.search({
        index: `dashboard_cells_${root["dashboardID"].toLowerCase()}`,
        body: query,
      });

      return results["hits"]["total"]["value"];
    },
    markers: (root) => root["markers"],
  },
};
