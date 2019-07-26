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
    Sample_ID: String
    Mito_5: Int!
    Mito_10: Int!
    Mito_15: Int!
    Mito_20: Int!
    Estimated_Number_of_Cells: Int!
    Number_of_Reads: Int!
    Number_of_Genes: Int!
    Mean_Reads_per_Cell: Int!
    Median_Genes_per_Cell: Int!
    Valid_Barcodes: String!
    Sequencing_Saturation: String!
    Median_UMI_Counts_per_Cell: Int!
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
    Sample_ID: root => root["sample_id"],
    Mito_5: root => root["mito5"],
    Mito_10: root => root["mito10"],
    Mito_15: root => root["mito15"],
    Mito_20: root => root["mito20"],
    Estimated_Number_of_Cells: root => root["num_cells"],
    Number_of_Reads: root => root["num_reads"],
    Number_of_Genes: root => root["num_genes"],
    Mean_Reads_per_Cell: root => root["mean_reads"],
    Median_Genes_per_Cell: root => root["median_genes"],
    Valid_Barcodes: root => root["percent_barcodes"],
    Sequencing_Saturation: root => root["sequencing_sat"],
    Median_UMI_Counts_per_Cell: root => root["median_umi"]
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
