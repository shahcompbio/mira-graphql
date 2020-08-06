const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import {
  getBinSizes,
  getBaseDensityQuery,
  getCellIDs,
  getDataMap,
  getRecords
} from "./density";

export const schema = gql`
  extend type Query {
    cumulativeGenes(
      dashboardID: String!
      genes: [String!]!
      highlightedGroup: AttributeInput
    ): [DensityBin!]!
    verifyGenes(dashboardID: String!, genes: [String!]!): GeneList!
  }

  type GeneList {
    valid: [String!]!
    invalid: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async verifyGenes(_, { dashboardID, genes }) {
      if (genes.length === 0) {
        return { valid: [], invalid: [] };
      }
      const query = bodybuilder()
        .size(0)
        .filter("terms", "gene", genes)
        .agg("terms", "gene", { size: 10000 })
        .build();

      const results = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: query
      });

      const validGenes = results["aggregations"]["agg_terms_gene"][
        "buckets"
      ].map(bucket => bucket["key"]);

      return {
        valid: genes.filter(gene => validGenes.indexOf(gene) !== -1),
        invalid: genes.filter(gene => validGenes.indexOf(gene) === -1)
      };
    },

    async cumulativeGenes(_, { dashboardID, genes, highlightedGroup }) {
      const { xBinSize, yBinSize } = await getBinSizes(dashboardID);

      // Query fetching
      let query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
        a.aggregation("stats", "log_count")
      ).filter("terms", "gene", genes);

      if (highlightedGroup) {
        const cellIDs = await getCellIDs(dashboardID, highlightedGroup);
        query = query.filter("terms", "cell_id", cellIDs);
      }

      const results = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: query.build()
      });

      const getValue = bucket => bucket["agg_stats_log_count"]["sum"];

      const dataMap = getDataMap(results, xBinSize, yBinSize, getValue);

      const records = await getRecords(
        dataMap,
        dashboardID,
        xBinSize,
        yBinSize,
        "",
        false
      );
      return records;
    }
  }
};
