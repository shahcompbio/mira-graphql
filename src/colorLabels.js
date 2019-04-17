const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    colorLabels(sampleID: String!): [ColorLabelGroup!]!
    colorLabelValues(
      sampleID: String!
      label: String!
      labelType: String!
    ): [ColorLabelValue!]!
  }
  type ColorLabelGroup {
    id: String!
    title: String!
    labels: [ColorLabel!]!
  }
  type ColorLabel {
    id: String!
    title: String!
    type: String!
  }
  type ColorLabelValue {
    id: ID!
    name: StringOrNum!
    count: Int!
  }
`;

export const resolvers = {
  Query: {
    async colorLabels(_, { sampleID }) {
      // TODO: Actually scrape some place to get these values
      const cellGroup = {
        id: "categorical",
        title: "Cell Properties",
        labels: [
          {
            id: "cell_type",
            title: "Cell Type",
            type: "categorical"
          },
          {
            id: "cluster",
            title: "Cluster",
            type: "categorical"
          }
        ]
      };

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
      ].map(bucket => ({
        id: bucket["key"],
        title: bucket["key"],
        type: "gene"
      }));

      const geneGroup = {
        id: "genes",
        title: "Genes",
        labels: geneResults
      };

      return [cellGroup, geneGroup];
    },
    async colorLabelValues(_, { sampleID, label, labelType }) {
      if (labelType === "gene") {
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

        const geneBuckets =
          results["aggregations"][`agg_terms_count`]["buckets"];
        const totalNumCells = await getTotalNumCells(sampleID);
        const numGeneCells = geneBuckets.reduce(
          (sum, bucket) => sum + bucket.doc_count,
          0
        );

        return [
          { key: 0, doc_count: totalNumCells - numGeneCells, sampleID, label },
          ...geneBuckets.map(bucket => ({ ...bucket, sampleID, label }))
        ];
      } else {
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
    }
  },
  ColorLabelGroup: {
    id: root => root.id,
    title: root => root.title,
    labels: root => root.labels
  },
  ColorLabel: {
    id: root => root.id,
    title: root => root.title,
    type: root => root.type
  },
  ColorLabelValue: {
    id: root => `${root.sampleID}_${root.label}_${root.key}`,
    name: root => root.key,
    count: root => root.doc_count
  }
};

async function getTotalNumCells(sampleID) {
  const query = bodybuilder()
    .filter("term", "sample_id", sampleID)
    .aggregation("cardinality", "cell_id")
    .build();
  const results = await client.search({
    index: "scrna_cells",
    body: query
  });

  return results["aggregations"]["agg_cardinality_cell_id"]["value"];
}
