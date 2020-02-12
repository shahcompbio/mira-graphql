const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    correlation(
      dashboardID: String!
      labels: [AttributeInput!]
    ): [CorrelationCell!]!
  }

  type CorrelationCell {
    x: StringOrNum!
    y: StringOrNum!
  }
`;

export const resolvers = {
  Query: {
    async correlation(_, { dashboardID, labels }) {
      return [];
    }
  }
};
