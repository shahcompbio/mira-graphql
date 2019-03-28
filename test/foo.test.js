import "@babel/polyfill";
import { schema, resolvers } from "../src/foo";
import { startServer } from "./test-server";
import { gql } from "apollo-server";

let query;
beforeAll(async () => {
  const client = await startServer(schema, resolvers);
  query = client.query;
});

it("has server started", () => {
  expect(query).toBeDefined();
});

it("test foo query", async () => {
  const FOO_QUERY = gql`
    query {
      foo {
        id
        name
        age
      }
    }
  `;

  const res = await query({ query: FOO_QUERY });
  expect(res).toMatchSnapshot();
});
