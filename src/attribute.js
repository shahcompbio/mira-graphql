const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";
import { getCellIDs } from "./density";
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
    value: StringOrNum
  }
`;

export const resolvers = {
  Query: {
    async attributes(_, { dashboardType, dashboardID }) {
      // PURPOSE: Get all possible coloring values for UMAP

      // Categorical values
      const CELL_CATEGORICAL = ["celltype"].map(label => ({
        isNum: false,
        type: "CELL",
        label
      }));

      const SAMPLE_CATEGORICAL = [
        "surgery",
        "site",
        "treatment",
        "sort"
      ].map(label => ({ label, isNum: false, type: "SAMPLE" }));

      // Numerical values
      const cellFields = await client.indices.getMapping({
        index: "dashboard_cells"
      });

      const CELL_NUMERICAL = Object.keys(
        cellFields["dashboard_cells"]["mappings"]["properties"]
      )
        .filter(
          field =>
            ![
              "dashboard_id",
              "cell_id",
              "cell_type",
              "x",
              "y",
              "sample_id"
            ].includes(field)
        )
        .map(label => ({ isNum: true, label, type: "CELL" }));

      const geneQuery = bodybuilder()
        .size(0)
        .agg("terms", "gene", { size: 50000, order: { _key: "asc" } })
        .build();

      const geneResults = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: geneQuery
      });

      const GENE_NUMERICAL = geneResults["aggregations"]["agg_terms_gene"][
        "buckets"
      ].map(bucket => ({ isNum: true, label: bucket["key"], type: "GENE" }));

      return [
        ...CELL_CATEGORICAL,
        ...(dashboardType.toLowerCase() === "sample" ? [] : SAMPLE_CATEGORICAL),
        ...CELL_NUMERICAL,
        ...GENE_NUMERICAL
      ];
    },

    async attributeCounts(_, { dashboardID, label, highlightedGroup }) {
      if (label["isNum"]) {
        if (label["type"] === "CELL") {
          return await getCellNumericalCounts(
            dashboardID,
            label,
            highlightedGroup
          );
        } else {
          // is "GENE"

          return await getGeneExpressionCounts(
            dashboardID,
            label,
            highlightedGroup
          );
        }
      } else {
        // is categorical
        if (label["type"] === "CELL") {
          return await getCelltypeCounts(dashboardID, label, highlightedGroup);
        } else {
          return await getSampleCounts(dashboardID, label, highlightedGroup);
        }
      }
    }
  }
};

async function getCelltypeCounts(dashboardID, label, highlightedGroup) {
  const celltypeQuery = bodybuilder()
    .size(0)
    .aggregation("terms", "celltype", { size: 50 })
    .build();

  const celltypeResults = await client.search({
    index: "rho_markers",
    body: celltypeQuery
  });

  const celltypes = [
    ...celltypeResults["aggregations"]["agg_terms_celltype"]["buckets"]
      .map(bucket => bucket["key"])
      .sort(),
    "Other"
  ];

  const cellIDs = !highlightedGroup
    ? []
    : await getCellIDs(dashboardID, highlightedGroup);

  const query = !highlightedGroup
    ? bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("terms", "cell_type", { size: 1000 })
        .build()
    : bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .filter("terms", "cell_id", cellIDs)
        .aggregation("terms", "cell_type", { size: 1000 })
        .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  const counts = results["aggregations"]["agg_terms_cell_type"][
    "buckets"
  ].reduce(
    (countsMap, bucket) => ({
      ...countsMap,
      [bucket["key"]]: bucket["doc_count"]
    }),
    {}
  );

  return celltypes.map(record => ({
    ...label,
    value: record,
    count: counts.hasOwnProperty(record) ? counts[record] : 0
  }));
}

async function getSampleCounts(dashboardID, label, highlightedGroup) {
  const sampleIDquery = bodybuilder()
    .size(1000)
    .filter("term", "patient_id", dashboardID)
    .filter("term", "type", "sample")
    .aggregation("terms", label["label"], { size: 1000 })
    .build();

  const sampleIDresults = await client.search({
    index: "dashboard_entry",
    body: sampleIDquery
  });

  const sampleIDMap = sampleIDresults["hits"]["hits"]
    .map(record => record["_source"])
    .reduce(
      (sampleMap, record) => ({
        ...sampleMap,
        [record["dashboard_id"]]: record[label["label"]][0]
      }),
      {}
    );

  const sampleCounts = sampleIDresults["aggregations"][
    `agg_terms_${label["label"]}`
  ]["buckets"]
    .map(bucket => bucket["key"])
    .sort();

  const cellIDs = !highlightedGroup
    ? []
    : await getCellIDs(dashboardID, highlightedGroup);

  const query = !highlightedGroup
    ? bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("terms", "sample_id", { size: 1000 })
        .build()
    : bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .filter("terms", "cell_id", cellIDs)
        .aggregation("terms", "sample_id", { size: 1000 })
        .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  const counts = results["aggregations"]["agg_terms_sample_id"][
    "buckets"
  ].reduce((countsMap, bucket) => {
    const sampleKey = sampleIDMap[bucket["key"]];

    return {
      ...countsMap,
      [sampleKey]: countsMap.hasOwnProperty(sampleKey)
        ? countsMap[sampleKey] + bucket["doc_count"]
        : bucket["doc_count"]
    };
  }, {});

  return sampleCounts.map(record => ({
    ...label,
    value: record,
    count: counts.hasOwnProperty(record) ? counts[record] : 0
  }));
}

async function getCellNumericalCounts(dashboardID, label, highlightedGroup) {
  const cellIDs = !highlightedGroup
    ? []
    : await getCellIDs(dashboardID, highlightedGroup);

  const query = !highlightedGroup
    ? bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("histogram", label["label"], {
          interval: 0.1,
          extended_bounds: { min: 0, max: 1 }
        })
        .build()
    : bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .filter("terms", "cell_id", cellIDs)
        .aggregation("histogram", label["label"], {
          interval: 0.1,
          extended_bounds: { min: 0, max: 1 }
        })
        .build();
  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  const records = results["aggregations"][`agg_histogram_${label["label"]}`][
    "buckets"
  ].map(bucket => ({
    ...label,
    value: bucket["key"],
    count: bucket["doc_count"]
  }));

  const lastRecord = {
    ...records[records.length - 2],
    count:
      records[records.length - 2]["count"] +
      records[records.length - 1]["count"]
  };

  return [...records.slice(0, records.length - 2), lastRecord];
}

async function getGeneExpressionCounts(dashboardID, label, highlightedGroup) {
  const minMaxQuery = bodybuilder()
    .size(0)
    .filter("term", "gene", label["label"])
    .aggregation("stats", "log_count")
    .build();

  const minMaxResults = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: minMaxQuery
  });

  const min = minMaxResults["aggregations"]["agg_stats_log_count"]["min"];
  const max = minMaxResults["aggregations"]["agg_stats_log_count"]["max"];
  const binSize = (max - min) / 10;

  const cellIDs = !highlightedGroup
    ? []
    : await getCellIDs(dashboardID, highlightedGroup);

  const query = !highlightedGroup
    ? bodybuilder()
        .size(0)
        .filter("term", "gene", label["label"])
        .aggregation("histogram", "log_count", {
          interval: binSize,
          extended_bounds: { min, max }
        })
        .build()
    : bodybuilder()
        .size(0)
        .filter("terms", "cell_id", cellIDs)
        .filter("term", "gene", label["label"])
        .aggregation("histogram", "log_count", {
          interval: binSize,
          extended_bounds: { min, max }
        })
        .build();
  const results = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: query
  });

  const records = results["aggregations"][`agg_histogram_log_count`][
    "buckets"
  ].map(bucket => ({
    ...label,
    value: bucket["key"],
    count: bucket["doc_count"]
  }));

  const lastRecord = {
    ...records[records.length - 2],
    count:
      records[records.length - 2]["count"] +
      records[records.length - 1]["count"]
  };

  return [...records.slice(0, records.length - 2), lastRecord];
}
