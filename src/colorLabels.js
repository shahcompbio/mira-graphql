const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

const util = require("util");
export const schema = gql`
  extend type Query {
    colorLabels(sampleID: String!): [ColorLabel!]!
    colorLabelValues(sampleID: String!, label: String!): [ColorLabelValue!]!
  }
  type ColorLabel {
    id: String!
    title: String!
  }
  type ColorLabelValue {
    id: ID!
    name: String!
    count: Int!
  }
`;

export const resolvers = {
  Query: {
    async colorLabels(_, { sampleID }) {
      // TODO: Actually scrape some place to get these values
      const geneQuery = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("terms", "gene", { size: 50000, order: { _key: "asc" } })
        .build();

      const results = await client.search({
        index: "scrna_genes",
        body: geneQuery
      });

      const geneResults = results["aggregations"][`agg_terms_gene`][
        "buckets"
      ].map(bucket => ({ id: bucket["key"], title: bucket["key"] }));
      return [
        {
          id: "cell_type",
          title: "Cell Type"
        },
        {
          id: "cluster",
          title: "Cluster"
        },
        ...geneResults
      ];
    },
    async colorLabelValues(_, { sampleID, label }) {
      if (label !== "cell_type" && label !== "cluster") {
        const query = bodybuilder()
          .size(0)
          .filter("term", "sample_id", sampleID)
          .filter("term", "gene", label)
          .aggregation("terms", "count", { order: { _key: "asc" } })
          .build();

        const results = await client.search({
          index: "scrna_genes",
          body: query
        });

        return [
          { key: 0, doc_count: 100, sampleID, label },
          ...results["aggregations"][`agg_terms_count`]["buckets"].map(
            bucket => ({ ...bucket, sampleID, label })
          )
        ];
      }
      const query = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("terms", label, { order: { _key: "asc" } })
        .build();

      const results = await client.search({
        index:
          label === "cell_type" || label === "cluster"
            ? "scrna_cells"
            : "scrna_genes",
        body: query
      });

      return results["aggregations"][`agg_terms_${label}`]["buckets"].map(
        bucket => ({ ...bucket, sampleID, label })
      );
    }
  },
  ColorLabel: {
    id: root => root.id,
    title: root => root.title
  },
  ColorLabelValue: {
    id: root => `${root.sampleID}_${root.label}_${root.key}`,
    name: root => root.key.toString(),
    count: root => root.doc_count
  }
};
