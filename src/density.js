const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    density(type: String!, dashboardID: String!): [DensityBin!]
  }

  type DensityBin {
    x: Int!
    y: Int!
    values: [DensityBinValue!]
  }

  type DensityBinValue {
    celltype: String!
    proportion: Float!
    count: Int!
  }
`;

export const resolvers = {
  Query: {
    async density(_, { type, dashboardID }) {
      const query = bodybuilder()
        .size(50000)
        .filter("term", "dashboard_id", dashboardID)
        .sort([{ x: "asc" }, { y: "asc" }, { count: "desc" }])
        .build();

      const results = await client.search({
        index: `dashboard_cells_density`,
        body: query
      });
      const records = condenseResults(
        results["hits"]["hits"].map(record => record["_source"])
      );
      return records;
    }
  },

  DensityBin: {
    x: root => root["x"],
    y: root => root["y"],
    values: root => {
      const total = root["values"].reduce(
        (total, record) => total + record["count"],
        0
      );

      return root["values"].map(record => ({ ...record, total }));
    }
  },

  DensityBinValue: {
    celltype: root => root["celltype"],
    proportion: root => root["count"] / root["total"],
    count: root => root["count"]
  }
};

const condenseResults = records => {
  const helper = (records, rsf, currXY, acc) => {
    if (records.length === 0) {
      return [...rsf, { ...currXY, values: acc }];
    } else {
      const [currRecord, ...restRecords] = records;
      if (currXY["x"] === currRecord["x"] && currXY["y"] === currRecord["y"]) {
        return helper(restRecords, rsf, currXY, [...acc, currRecord]);
      } else {
        return helper(
          restRecords,
          [...rsf, { ...currXY, values: acc }],
          currRecord,
          [currRecord]
        );
      }
    }
  };
  const [firstRecord, ...restRecords] = records;
  return helper(restRecords, [], firstRecord, [firstRecord]);
};
