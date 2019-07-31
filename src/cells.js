const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cells(patientID: String!, sampleID: String!, label: String): [Cell]
    nonGeneCells(patientID: String!, sampleID: String): [ModCell!]!
    patientCells(patientID: String!, label: String): [PatientCell]
  }

  type PatientCell {
    name: String!
    x: Float!
    y: Float!
    label: Float!
    celltype: String!
    site: String!
  }

  type ModCell {
    x: Float!
    y: Float!
    celltype: String!
  }

  type Cell {
    id: ID!
    name: String!
    x: Float!
    y: Float!
    label: Float!
    celltype: String!
  }
`;

export const resolvers = {
  Query: {
    async patientCells(_, { patientID, label }) {
      const query = bodybuilder()
        .notFilter("exists", "sample_id")
        .size(50000)
        .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_cells`,
        body: query
      });

      const geneQuery = bodybuilder()
        .size(50000)
        .notFilter("exists", "sample_id")
        .filter("term", "gene", label)
        .build();

      const geneResults = await client.search({
        index: `${patientID.toLowerCase()}_genes`,
        body: geneQuery
      });

      const geneRecords = geneResults.hits.hits.reduce((geneMap, hit) => ({
        ...geneMap,
        [hit["_source"]["cell_id"]]: hit["_source"]["count"]
      }));

      const genesArray = results.hits.hits.map(hit => ({
        ...hit["_source"],
        label: geneRecords.hasOwnProperty(hit["_source"]["cell_id"])
          ? geneRecords[hit["_source"]["cell_id"]]
          : 0
      }));

      const cellTypesArray = results.hits.hits.map(hit => ({
        ...hit["_source"],
        label: hit["_source"]["cell_type"]
      }));

      const finalArray = genesArray.map(element => ({
        cell_id: element["cell_id"],
        x: element["x"],
        y: element["y"],
        label: element["label"],
        celltype: cellTypesArray[genesArray.indexOf(element)]["cell_type"],
        site: cellTypesArray[genesArray.indexOf(element)]["site"]
      }));

      return finalArray;
    },

    async cells(_, { patientID, sampleID, label }) {
      const query = bodybuilder()
        .size(50000)
        .filter("term", "sample_id", sampleID)
        .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_cells`,
        body: query
      });
      const geneQuery = bodybuilder()
        .size(50000)
        .filter("term", "sample_id", sampleID)
        .filter("term", "gene", label)
        .build();

      const geneResults = await client.search({
        index: `${patientID.toLowerCase()}_genes`,
        body: geneQuery
      });

      const geneRecords = geneResults.hits.hits.reduce((geneMap, hit) => ({
        ...geneMap,
        [hit["_source"]["cell_id"]]: hit["_source"]["count"]
      }));

      const genesArray = results.hits.hits.map(hit => ({
        ...hit["_source"],
        label: geneRecords.hasOwnProperty(hit["_source"]["cell_id"])
          ? geneRecords[hit["_source"]["cell_id"]]
          : 0
      }));

      const cellTypesArray = results.hits.hits.map(hit => ({
        ...hit["_source"],
        label: hit["_source"]["cell_type"]
      }));

      const finalArray = genesArray.map(element => ({
        cell_id: element["cell_id"],
        sample_id: element["sample_id"],
        x: element["x"],
        y: element["y"],
        label: element["label"],
        celltype: cellTypesArray[genesArray.indexOf(element)]["cell_type"]
      }));
      return finalArray;
    },

    async nonGeneCells(_, { patientID, sampleID }) {
      const query =
        sampleID === undefined
          ? bodybuilder()
              .size(50000)
              .notFilter("exists", "sample_id")
              .build()
          : bodybuilder()
              .size(50000)
              .filter("term", "sample_id", sampleID)
              .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_cells`,
        body: query
      });

      return results.hits.hits.map(hit => hit["_source"]);
    }
  },

  ModCell: {
    x: root => root["x"],
    y: root => root["y"],
    celltype: root => root["cell_type"]
  },

  PatientCell: {
    name: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    label: root => root["label"],
    celltype: root => root["celltype"],
    site: root => root["site"]
  },

  Cell: {
    name: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    label: root => root["label"],
    celltype: root => root["celltype"]
  }
};
