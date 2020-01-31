const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    density(
      dashboardID: String!
      label: DashboardAttributeInput!
      highlightedGroup: String
    ): [DensityBin!]
  }

  type DensityBin {
    x: Float!
    y: Float!
    label: String!
    value: StringOrNum!
  }
`;

export const resolvers = {
  Query: {
    async density(_, { dashboardID, highlightedGroup, label }) {
      const sizeQuery = bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("stats", "x")
        .aggregation("stats", "y")
        .build();

      const sizeResults = await client.search({
        index: "dashboard_cells",
        body: sizeQuery
      });

      const { agg_stats_x, agg_stats_y } = sizeResults["aggregations"];

      const xBinSize = (agg_stats_x["max"] - agg_stats_x["min"]) / 100;
      const yBinSize = (agg_stats_y["max"] - agg_stats_y["min"]) / 100;
      const data = await getBinnedData(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );
      return data;
    }
  }
};

async function getBinnedData(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  if (label["label"] === "celltype") {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("terms", "cell_type", { size: 1000 })
    );

    const results = await client.search({
      index: "dashboard_cells",
      body: query.build()
    });

    const records = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          !highlightedGroup ? label["label"] : highlightedGroup,
          !highlightedGroup
            ? yBucket => yBucket["agg_terms_cell_type"]["buckets"][0]["key"]
            : yBucket =>
                calculateProportion(
                  yBucket["agg_terms_cell_type"]["buckets"],
                  highlightedGroup
                )
        )
      ],
      []
    );

    return records;
  } else if (label["type"] === "CELL") {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("stats", label["label"])
    );

    const results = await client.search({
      index: "dashboard_cells",
      body: query.build()
    });

    const filteredBins = await getFilteredBins(
      highlightedGroup,
      dashboardID,
      xBinSize,
      yBinSize,
      label["label"]
    );

    const records = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          label["label"],
          yBucket => {
            const xEncoded = Math.round(xBucket["key"] / xBinSize);
            const yEncoded = Math.round(yBucket["key"] / yBinSize);

            return filteredBins.hasOwnProperty(xEncoded) &&
              filteredBins[xEncoded].hasOwnProperty(yEncoded)
              ? filteredBins[xEncoded][yEncoded]
              : !highlightedGroup
              ? yBucket[`agg_stats_${label["label"]}`]["avg"]
              : 0;
          }
        )
      ],
      []
    );

    return records;
  } else if (label["type"] === "SAMPLE") {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("terms", "sample_id", { size: 1000 })
    );

    const results = await client.search({
      index: "dashboard_cells",
      body: query.build()
    });

    const [patientID, sortID] = dashboardID.split("_");
    const metadataQuery = bodybuilder()
      .size(1000)
      .filter("term", "patient_id", patientID)
      .filter("term", "sort", sortID)
      .build();

    const metadataResults = await client.search({
      index: "dashboard_entry",
      body: metadataQuery
    });

    const metadata = metadataResults["hits"]["hits"]
      .map(record => record["_source"])
      .reduce(
        (metadataMap, record) => ({
          ...metadataMap,
          [record["dashboard_id"]]: record
        }),
        {}
      );

    const records = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          !highlightedGroup ? "sample_id" : highlightedGroup,
          !highlightedGroup
            ? yBucket =>
                metadata[yBucket[`agg_terms_sample_id`]["buckets"][0]["key"]][
                  label["label"]
                ][0]
            : yBucket =>
                calculateProportion(
                  yBucket[`agg_terms_sample_id`]["buckets"].map(record => ({
                    ...record,
                    key: metadata[record["key"]][label["label"]][0]
                  })),
                  highlightedGroup
                )
        )
      ],
      []
    );

    return records;
  } else {
    // Is genes
  }
}

const getBaseDensityQuery = (dashboardID, xBinSize, yBinSize, labelAgg) =>
  bodybuilder()
    .size(0)
    .filter("term", "dashboard_id", dashboardID)
    .aggregation(
      "histogram",
      "x",
      { interval: xBinSize, min_doc_count: 1 },
      a =>
        a.aggregation(
          "histogram",
          "y",
          { interval: yBinSize, min_doc_count: 1 },
          labelAgg
        )
    );

const processXBuckets = (xBucket, xBinSize, yBinSize, label, getValue) =>
  xBucket["agg_histogram_y"]["buckets"].map(yBucket => {
    return {
      x: Math.round(xBucket["key"] / xBinSize),
      y: Math.round(yBucket["key"] / yBinSize),
      value: getValue(yBucket),
      label
    };
  });

const calculateProportion = (counts, highlightedGroup) => {
  const total = counts.reduce(
    (currSum, record) => currSum + record["doc_count"],
    0
  );

  const filteredRecords = counts.filter(
    record => record["key"] === highlightedGroup
  );

  return filteredRecords.length === 0
    ? 0
    : filteredRecords[0]["doc_count"] / total;
};

async function getFilteredBins(
  highlightedGroup,
  dashboardID,
  xBinSize,
  yBinSize,
  label
) {
  if (highlightedGroup) {
    const filteredQuery = bodybuilder()
      .size(0)
      .filter("term", "dashboard_id", dashboardID)
      .filter("term", "cell_type", highlightedGroup)
      .aggregation(
        "histogram",
        "x",
        { interval: xBinSize, min_doc_count: 1 },
        a =>
          a.aggregation(
            "histogram",
            "y",
            { interval: yBinSize, min_doc_count: 1 },
            a => a.aggregation("stats", label)
          )
      )
      .build();

    const filteredResults = await client.search({
      index: "dashboard_cells",
      body: filteredQuery
    });

    const processProbYBucket = yBuckets =>
      yBuckets.reduce(
        (yMap, bucket) => ({
          ...yMap,
          [Math.round(bucket["key"] / yBinSize)]: bucket[`agg_stats_${label}`][
            "avg"
          ]
        }),
        {}
      );

    return filteredResults["aggregations"]["agg_histogram_x"]["buckets"].reduce(
      (xMap, bucket) => ({
        ...xMap,
        [Math.round(bucket["key"] / xBinSize)]: processProbYBucket(
          bucket["agg_histogram_y"]["buckets"]
        )
      }),
      {}
    );
  } else {
    return {};
  }
}
