const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";
import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cellAndMarkerGenesPair(patientID: String!): [Rho!]!
    existingCellTypes(patientID: String!, sampleID: String!): [Pairs!]
    qcTableValues(patientID: String!): [TableValue!]!
  }

  type Pairs {
    cell: String
    count: Int
  }

  type Rho {
    cellType: String!
    markerGenes: [String!]!
  }

  type TableValue {
    sample_id: String
    mito5: Int!
    mito10: Int!
    mito15: Int!
    mito20: Int!
    num_cells: Int!
    num_reads: Int!
    num_genes: Int!
    mean_reads: Int!
    median_genes: Int!
    percent_barcodes: String!
    sequencing_sat: String!
    median_umi: Int!
  }
`;

export const resolvers = {
  Query: {
    async cellAndMarkerGenesPair(_, { patientID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "patient_id", patientID)
        .aggregation("terms", "celltype", { size: 100 }, a => {
          return a.aggregation("terms", "marker_gene", { size: 100 });
        })
        .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_rho`,
        body: query
      });

      return results["aggregations"]["agg_terms_celltype"]["buckets"];
    },

    async existingCellTypes(_, { patientID, sampleID }) {
      const query = bodybuilder()
        .size(0)
        .filter("term", "sample_id", sampleID)
        .aggregation("terms", "cell_type", { size: 100 })
        .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_cells`,
        body: query
      });

      const sorted = results["aggregations"]["agg_terms_cell_type"]["buckets"]
        .map(element => element.key)
        .sort();

      return sorted.map(
        cellType =>
          results["aggregations"]["agg_terms_cell_type"]["buckets"].filter(
            element => element.key === cellType
          )[0]
      );
    },
    async qcTableValues(_, { patientID }) {
      const query = bodybuilder()
        .size(5000)
        .filter("term", "patient_id", patientID)
        .build();

      const results = await client.search({
        index: "patient_metadata",
        body: query
      });

      return results.hits.hits.map(element => element["_source"]);
    }
  },

  TableValue: {
    sample_id: root => root["sample_id"],
    mito5: root => root["mito5"],
    mito10: root => root["mito10"],
    mito15: root => root["mito15"],
    mito20: root => root["mito20"],
    num_cells: root => root["num_cells"],
    num_reads: root => root["num_reads"],
    num_genes: root => root["num_genes"],
    mean_reads: root => root["mean_reads"],
    median_genes: root => root["median_genes"],
    percent_barcodes: root => root["percent_barcodes"],
    sequencing_sat: root => root["sequencing_sat"],
    median_umi: root => root["median_umi"]
  },

  Pairs: {
    cell: root => root.key,
    count: root => root["doc_count"]
  },

  Rho: {
    cellType: root => root.key,
    markerGenes: root =>
      root["agg_terms_marker_gene"]["buckets"].map(marker => marker.key)
  }
};
