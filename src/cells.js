const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

const threshold = 0.25;

export const schema = gql`
  extend type Query {
    cells(patientID: String!, sampleID: String, label: String): [Cell]
    sites(patientID: String!): [String!]!
  }

  type Cell {
    id: ID
    name: String
    x: Float!
    y: Float!
    label: Float
    celltype: String
    site: String
  }
`;

export const resolvers = {
  Query: {
    async sites(_, { patientID }) {
      const query = bodybuilder()
        .size(0)
        .notFilter("exists", "sample_id")
        .aggregation("terms", "site", { size: 50 })
        .build();

      const results = await client.search({
        index: `${patientID.toLowerCase()}_cells`,
        body: query
      });

      return results["aggregations"]["agg_terms_site"]["buckets"]
        .map(element => element.key)
        .sort();
    },
    async cells(_, { patientID, sampleID, label }) {
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

      if (label === "Site") {
        return results.hits.hits.map(element => ({
          site: element["_source"]["site"],
          celltype: element["_source"]["cell_type"],
          x: element["_source"]["x"],
          y: element["_source"]["y"]
        }));
      }

      if (label !== undefined) {
        const geneQuery =
          sampleID === undefined
            ? bodybuilder()
                .size(50000)
                .notFilter("exists", "sample_id")
                .filter("term", "gene", label)
                .build()
            : bodybuilder()
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
          celltype: cellTypesArray[genesArray.indexOf(element)]["cell_type"],
          site:
            sampleID === undefined
              ? cellTypesArray[genesArray.indexOf(element)]["site"]
              : null
        }));

        let newArr = finalArray;

        for (let i = 0; i < finalArray.length; i++) {
          for (let j = 0; j < finalArray.length; j++) {
            if (
              finalArray[i] !== finalArray[j] &&
              Math.abs(finalArray[i].x - finalArray[j].x) < threshold &&
              Math.abs(finalArray[i].y - finalArray[j].y) < threshold &&
              finalArray[i].celltype === finalArray[j].celltype
            ) {
              newArr.splice(j, 1);
            }
          }
        }

        return newArr;
      } else {
        const finalArray = results.hits.hits
          .map(hit => hit["_source"])
          .map(element => ({
            celltype: element["cell_type"],
            x: element["x"],
            y: element["y"]
          }));

        let newArr = finalArray;

        for (let i = 0; i < finalArray.length; i++) {
          for (let j = 0; j < finalArray.length; j++) {
            if (
              finalArray[i] !== finalArray[j] &&
              Math.abs(finalArray[i].x - finalArray[j].x) < threshold &&
              Math.abs(finalArray[i].y - finalArray[j].y) < threshold &&
              finalArray[i].celltype === finalArray[j].celltype
            ) {
              newArr.splice(j, 1);
            }
          }
        }

        return newArr;
      }
    }
  },

  Cell: {
    name: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    label: root => root["label"],
    celltype: root => root["celltype"],
    site: root => root["site"]
  }
};
