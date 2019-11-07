const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import getSampleIDs from "./utils/getSampleIDs.js";

export const schema = gql`
  extend type Query {
    cells(
      type: String!
      dashboardID: String!
      props: [DashboardAttributeInput!]!
    ): [Cell!]!
    dashboardCellAttributes(
      type: String!
      dashboardID: String!
    ): [DashboardAttribute!]!
    dashboardAttributeValues(
      type: String!
      dashboardID: String!
      prop: DashboardAttributeInput!
    ): [DashboardAttributeValue!]!
  }

  type Cell {
    id: ID
    name: String
    x: Float
    y: Float
    celltype: String
    values: [CellAttribute]
  }

  type CellAttribute {
    label: String
    value: StringOrNum
  }

  type DashboardAttribute {
    label: String!
    type: String!
  }

  type DashboardAttributeValue {
    label: StringOrNum!
    count: Int
  }

  input DashboardAttributeInput {
    label: String!
    type: String!
  }
`;

export const resolvers = {
  Query: {
    async cells(_, { type, dashboardID, props }) {
      const sampleIDs = await getSampleIDs(type, dashboardID);

      const { geneProps, cellProps } = separateProps(props);

      const query = bodybuilder()
        .size(10000)
        .filter("terms", "sample_id", sampleIDs)
        .build();

      const results = await client.search({
        index: "sample_cells",
        body: query
      });

      const redimQuery = bodybuilder()
        .size(10000)
        .build();
      const redimResults = await client.search({
        index: `dashboard_redim_${dashboardID.toLowerCase()}`,
        body: redimQuery
      });

      const redimMap = redimResults["hits"]["hits"].reduce(
        (mapping, record) => ({
          ...mapping,
          [record["_source"]["cell_id"]]: record["_source"]
        }),
        {}
      );

      if (geneProps.length > 0) {
        const genesQuery = bodybuilder()
          .size(10000)
          .filter("term", "dashboard_id", dashboardID)
          .filter("terms", "gene", geneProps)
          .build();

        const geneResults = await client.search({
          index: `dashboard_genes_${dashboardID.toLowerCase()}`,
          body: genesQuery
        });

        const geneMapping = geneResults["hits"]["hits"]
          .map(record => record["_source"])
          .reduce((geneMap, record) => {
            const cellID = record["cell_id"];
            if (geneMap.hasOwnProperty(cellID)) {
              const cellRecords = geneMap[cellID];

              return {
                ...geneMap,
                [cellID]: { ...cellRecords, [record["gene"]]: record }
              };
            } else {
              return { ...geneMap, [cellID]: { [record["gene"]]: record } };
            }
          }, {});

        return results["hits"]["hits"].map(record => ({
          ...record["_source"],
          cellProps,
          geneProps,
          geneMapping,
          redimMap
        }));
      }
      return results["hits"]["hits"].map(record => ({
        ...record["_source"],
        redimMap,
        cellProps,
        geneProps
      }));
    },

    async dashboardCellAttributes(_, { type, dashboardID }) {
      const cellFields = await client.indices.getMapping({
        index: "sample_cells"
      });

      const cellAttributes = Object.keys(
        cellFields["sample_cells"]["mappings"]["properties"]
      )
        .filter(field => !["sample_id", "cell_id", "cell_type"].includes(field))
        .map(field => ({ label: field, type: "CELL" }));

      const geneQuery = bodybuilder()
        .size(0)
        .agg("terms", "gene", { size: 50000, order: { _key: "asc" } })
        .build();

      const geneResults = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: geneQuery
      });

      const geneAttributes = geneResults["aggregations"]["agg_terms_gene"][
        "buckets"
      ].map(bucket => ({ label: bucket["key"], type: "GENE" }));

      return [
        { label: "celltype", type: "CELL" },
        ...cellAttributes,
        ...geneAttributes
      ];
    },

    async dashboardAttributeValues(_, { type, dashboardID, prop }) {
      if (prop["label"] === "celltype") {
        const query = bodybuilder()
          .size(0)
          .agg("terms", "celltype", { size: 50 }, a => {
            return a.aggregation("terms", "marker", { size: 50 });
          })
          .build();

        const results = await client.search({
          index: "rho_markers",
          body: query
        });

        return results["aggregations"]["agg_terms_celltype"]["buckets"].sort(
          (a, b) => (a["key"] < b["key"] ? -1 : 1)
        );
      } else if (prop["type"] === "CELL") {
        const sampleIDs = await getSampleIDs(type, dashboardID);

        const query = bodybuilder()
          .size(0)
          .filter("terms", "sample_id", sampleIDs)
          .agg(
            "histogram",
            prop["label"],
            { interval: 0.1, extended_bounds: { min: 0, max: 1 } },
            "agg_histogram"
          )
          .build();

        const results = await client.search({
          index: "sample_cells",
          body: query
        });

        return results["aggregations"][`agg_histogram`]["buckets"];
      } else {
        const query = bodybuilder()
          .size(0)
          .filter("term", "gene", prop["label"])
          .agg("histogram", "log_count", { interval: 1 })
          .build();

        const results = await client.search({
          index: `dashboard_genes_${dashboardID.toLowerCase()}`,
          body: query
        });

        return results["aggregations"]["agg_histogram_log_count"]["buckets"];
      }
    }
  },

  Cell: {
    id: root => root["cell_id"],
    name: root => root["cell_id"],
    x: root => root["redimMap"][root["cell_id"]]["x"],
    y: root => root["redimMap"][root["cell_id"]]["y"],
    celltype: root => root["cell_type"],
    values: root => [
      ...root["cellProps"].map(prop => ({
        label: prop,
        value: root[prop]
      })),
      ...root["geneProps"].map(prop => ({
        label: prop,
        value:
          root["geneMapping"].hasOwnProperty(root["cell_id"]) &&
          root["geneMapping"][root["cell_id"]].hasOwnProperty(prop)
            ? root["geneMapping"][root["cell_id"]][prop]["log_count"]
            : 0
      }))
    ]
  },

  CellAttribute: {
    label: root => root["label"],
    value: root => root["value"]
  },

  DashboardAttribute: {
    label: root => root["label"],
    type: root => root["type"]
  },

  DashboardAttributeValue: {
    label: root => root["key"],
    count: root => root["doc_count"]
  }
};

const separateProps = props =>
  props
    .filter(prop => prop["label"] !== "celltype")
    .reduce(
      ({ geneProps, cellProps }, prop) =>
        prop["type"] === "GENE"
          ? { geneProps: [...geneProps, prop["label"]], cellProps }
          : { cellProps: [...cellProps, prop["label"]], geneProps },
      { geneProps: [], cellProps: [] }
    );
