import "@babel/polyfill";

const { ApolloServer, gql } = require("apollo-server");
const { createTestClient } = require("apollo-server-testing");

const baseSchema = gql`
  type Query {
    _blank: String
  }
`;

export async function startServer(schema, resolvers) {
  const server = await new ApolloServer({
    typeDefs: [baseSchema, schema],
    resolvers
  });

  return createTestClient(server);
}
