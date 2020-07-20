const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";
import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    attributeCounts(
      dashboardID: String!
      label: AttributeInput!
      highlightedGroup: AttributeInput
    ): [AttributeValue!]

    attributes(dashboardType: String!, dashboardID: String!): [Attribute!]!
  }
  type Attribute {
    isNum: Boolean!
    type: String!
    label: String!
  }

  type AttributeValue {
    isNum: Boolean!
    type: String!
    label: String!
    value: StringOrNum!
    count: Int!
  }

  input AttributeInput {
    isNum: Boolean!
    type: String!
    label: String!
    value: [StringOrNum!]
  }
`;

export const resolvers = {
  Query: {
    async attributes(_, { dashboardType, dashboardID }) {
      // PURPOSE: Get all possible coloring values for UMAP

      const CATEGORICAL = [
        "cell_type",
        "surgery",
        "site",
        "therapy",
        "sort",
        "sample_id",
      ].map((label) => ({
        isNum: false,
        type: "CELL",
        label,
      }));

      const geneQuery = bodybuilder().size(50000).build();

      const geneResults = await client.search({
        index: `genes`,
        body: geneQuery,
      });

      const GENE_NUMERICAL = geneResults["hits"]["hits"].map((record) => ({
        isNum: true,
        label: record["_source"]["gene"],
        type: "GENE",
      }));

      return [...CATEGORICAL, ...GENE_NUMERICAL];
    },

    async attributeCounts(_, { dashboardID, label, highlightedGroup }) {
      if (label["isNum"]) {
        // is "GENE"
        return await getGeneExpressionCounts(
          dashboardID,
          label,
          highlightedGroup
        );
      } else {
        return await getCellCategoricalCount(
          dashboardID,
          label,
          highlightedGroup
        );
      }
    },
  },
};

async function getCellCategoricalCount(dashboardID, label, highlightedGroup) {
  const baseQuery = bodybuilder()
    .size(0)
    .aggregation("terms", label["label"], {
      size: 1000,
      order: {
        _term: "asc",
      },
    });

  const query = highlightedGroup
    ? addFilter(
        bodybuilder()
          .size(0)
          .aggregation("terms", label["label"], {
            size: 1000,
            order: {
              _term: "asc",
            },
          }),
        highlightedGroup
      )
    : baseQuery;

  const baseResults = await client.search({
    index: `dashboard_cells_${dashboardID.toLowerCase()}`,
    body: baseQuery.build(),
  });

  const countResults = await client.search({
    index: `dashboard_cells_${dashboardID.toLowerCase()}`,
    body: query.build(),
  });

  return interleave(
    label,
    baseResults["aggregations"][`agg_terms_${label["label"]}`]["buckets"],
    countResults["aggregations"][`agg_terms_${label["label"]}`]["buckets"]
  );
}

async function getGeneExpressionCounts(dashboardID, label, highlightedGroup) {
  const minMaxQuery = bodybuilder()
    .size(0)
    .agg("nested", { path: "genes" }, "agg_genes", (a) =>
      a.agg(
        "filter",
        { term: { "genes.gene": label["label"] } },
        "agg_gene_filter",
        (a) => a.agg("stats", "genes.log_count")
      )
    )
    .build();

  const minMaxResults = await client.search({
    index: `dashboard_cells_${dashboardID.toLowerCase()}`,
    body: minMaxQuery,
  });

  const { min, max } = minMaxResults["aggregations"]["agg_genes"][
    "agg_gene_filter"
  ]["agg_stats_genes.log_count"];

  const binSize = (max - min) / 10;

  const baseQuery = bodybuilder()
    .size(0)
    .agg("nested", { path: "genes" }, "agg_genes", (a) =>
      a.agg(
        "filter",
        { term: { "genes.gene": label["label"] } },
        "agg_gene_filter",
        (a) =>
          a.agg("histogram", "genes.log_count", {
            interval: binSize,
            extended_bounds: { min, max },
          })
      )
    );

  const query = highlightedGroup
    ? addFilter(
        bodybuilder()
          .size(0)
          .agg("nested", { path: "genes" }, "agg_genes", (a) =>
            a.agg(
              "filter",
              { term: { "genes.gene": label["label"] } },
              "agg_gene_filter",
              (a) =>
                a.agg("histogram", "genes.log_count", {
                  interval: binSize,
                  extended_bounds: { min, max },
                })
            )
          ),
        highlightedGroup
      )
    : baseQuery;

  const results = await client.search({
    index: `dashboard_cells_${dashboardID.toLowerCase()}`,
    body: query.build(),
  });

  const records = results["aggregations"]["agg_genes"]["agg_gene_filter"][
    `agg_histogram_genes.log_count`
  ]["buckets"].map((bucket) => ({
    ...label,
    value: bucket["key"],
    count: bucket["doc_count"],
  }));

  const lastRecord = {
    ...records[records.length - 2],
    count:
      records[records.length - 2]["count"] +
      records[records.length - 1]["count"],
  };

  return [...records.slice(0, records.length - 2), lastRecord];
}

const addFilter = (query, filter) => {
  if (filter["isNum"]) {
    // Is gene

    return query.query("nested", "path", "genes", (q) =>
      q
        .query("match", "genes.gene", filter["label"])
        .query("range", "genes.log_count", {
          gte: filter["value"][0],
          lt: filter["value"][1],
        })
    );
  } else {
    return query.filter("terms", filter["label"], filter["value"]);
  }
};

const interleave = (label, baseArr, countArr) => {
  const countMap = countArr.reduce(
    (countMap, ele) => ({ ...countMap, [ele["key"]]: ele["doc_count"] }),
    {}
  );

  return baseArr.map((ele) => ({
    ...label,
    value: ele["key"],
    count: countMap.hasOwnProperty(ele["key"]) ? countMap[ele["key"]] : 0,
  }));
};
