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
  interface ColorLabelValue {
    id: ID!
    name: StringOrNum!
    count: Int!
  }
  type Categorical implements ColorLabelValue {
    id: ID!
    name: StringOrNum!
    count: Int!
  }
  type Gene implements ColorLabelValue {
    id: ID!
    name: StringOrNum!
    count: Int!
    min: Int!
    max: Int!
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
        const rangeQuery = bodybuilder()
          .size(0)
          .filter("term", "sample_id", sampleID)
          .filter("term", "gene", label)
          .aggregation("max", "log_count")
          .build();

        const rangeResults = await client.search({
          index: "scrna_genes",
          body: rangeQuery
        });

        const maxCount =
          rangeResults["aggregations"]["agg_max_log_count"]["value"];

        const histoInterval = getHistogramInterval(maxCount);

        const histoquery = bodybuilder()
          .size(0)
          .filter("term", "sample_id", sampleID)
          .filter("term", "gene", label)
          .aggregation("histogram", "log_count", { interval: histoInterval })
          .build();

        const histoResults = await client.search({
          index: "scrna_genes",
          body: histoquery
        });

        const geneBuckets =
          histoResults["aggregations"]["agg_histogram_log_count"]["buckets"];
        const totalNumCells = await getTotalNumCells(sampleID);
        const numGeneCells = geneBuckets.reduce(
          (sum, bucket) => sum + bucket.doc_count,
          0
        );
        const numZeroCountCells = totalNumCells - numGeneCells;

        const [firstBucket, ...restBucket] = geneBuckets;

        return [
          {
            min: 0,
            max: histoInterval - 1,
            sampleID,
            label,
            doc_count: firstBucket.doc_count + numZeroCountCells
          },
          ...restBucket.map(bucket => ({
            ...bucket,
            sampleID,
            label,
            min: bucket.key,
            max: bucket.key + histoInterval - 1
          }))
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
    __resolveType(obj, context, info) {
      if (obj.hasOwnProperty("min")) {
        return "Gene";
      } else {
        return "Categorical";
      }
    }
  },
  Categorical: {
    id: root => `${root.sampleID}_${root.label}_${root.key}`,
    name: root => root.key,
    count: root => root.doc_count
  },
  Gene: {
    id: root => `${root.sampleID}_${root.label}_${root.min}`,
    name: root => `${root.min} - ${root.max}`,
    count: root => root.doc_count,
    min: root => root.min,
    max: root => root.max
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

const getHistogramInterval = max => {
  const histogramIntervalAcc = i => {
    const intervalValue = i === 0 ? 1 : i * 5;
    if (max / intervalValue < 15) {
      return intervalValue;
    } else {
      return histogramIntervalAcc(i + 1);
    }
  };

  return histogramIntervalAcc(0);
};
