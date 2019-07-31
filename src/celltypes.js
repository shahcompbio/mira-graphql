const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";
import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cellAndMarkerGenesPair(patientID: String!): [Rho!]!
    existingCellTypes(patientID: String!, sampleID: String): [Pairs!]
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
    sampleID: Values!
    numCells: Values!
    mito20: Values!
    numReads: Values!
    numGenes: Values!
    medianGenes: Values!
    meanReads: Values!
    validBarcodes: Values!
    seqSat: Values!
    medUMI: Values!
  }

  type Values {
    name: String!
    value: StringOrNum!
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
      const query =
        sampleID === undefined
          ? bodybuilder()
              .size(0)
              .aggregation("terms", "cell_type", { size: 100 })
              .build()
          : bodybuilder()
              .size(0)
              .filter("term", "sample_id", sampleID)
              .aggregation("terms", "cell_type", { size: 100 })
              .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_cells`,
        body: query
      });

      const sorted =
        sampleID === undefined
          ? results["aggregations"]["agg_terms_cell_type"]["buckets"]
              .filter(element => !element.hasOwnProperty("sample_id"))
              .map(element => element.key)
              .sort()
          : results["aggregations"]["agg_terms_cell_type"]["buckets"]
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

      return results.hits.hits
        .map(element => element["_source"])
        .filter(element => element.hasOwnProperty("sample_id"));
    }
  },

  TableValue: {
    sampleID: root => ({ name: "Sample ID", value: root["sample_id"] }),
    numCells: root => ({
      name: "Estimated Number of Cells",
      value: root["num_cells"]
    }),
    mito20: root => ({ name: "QC (Mito<20)", value: root["mito20"] }),
    numReads: root => ({
      name: "Number of Reads",
      value: root["num_reads"]
    }),
    numGenes: root => ({
      name: "Number of Genes",
      value: root["num_genes"]
    }),
    meanReads: root => ({
      name: "Mean Reads per Cell",
      value: root["mean_reads"]
    }),
    medianGenes: root => ({
      name: "Median Genes per Cell",
      value: root["median_genes"]
    }),
    validBarcodes: root => ({
      name: "Valid Barcodes",
      value: root["percent_barcodes"]
    }),
    seqSat: root => ({
      name: "Sequencing Saturation",
      value: root["sequencing_sat"]
    }),
    medUMI: root => ({
      name: "Median UMI Counts per Cell",
      value: root["median_umi"]
    })
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
