const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    density(
      dashboardID: String!
      label: AttributeInput!
      highlightedGroup: AttributeInput
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
      const { xBinSize, yBinSize } = await getBinSizes(dashboardID);

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
  if (label["isNum"]) {
    if (label["type"] === "CELL") {
      const dataMap = await getCellNumericalBins(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );

      const records = await getRecords(
        dataMap,
        dashboardID,
        xBinSize,
        yBinSize,
        isSameLabel(label, highlightedGroup) ? "density" : label["label"],
        isSameLabel(label, highlightedGroup)
      );
      return records;
    } else {
      // is "GENE"
      const dataMap = await getGeneBins(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );

      const records = await getRecords(
        dataMap,
        dashboardID,
        xBinSize,
        yBinSize,
        isSameLabel(label, highlightedGroup) ? "density" : label["label"],
        true
      );
      return records;
    }
  } else {
    // is categorical
    if (label["type"] === "CELL") {
      const dataMap = await getCelltypeBins(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );

      const records = await getRecords(
        dataMap,
        dashboardID,
        xBinSize,
        yBinSize,
        isSameLabel(label, highlightedGroup) ? "density" : label["label"],
        isSameLabel(label, highlightedGroup)
      );
      return records;
    } else {
      // is "SAMPLE"
      const dataMap = await getSampleBins(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );

      const records = await getRecords(
        dataMap,
        dashboardID,
        xBinSize,
        yBinSize,
        isSameLabel(label, highlightedGroup) ? "density" : label["label"],
        isSameLabel(label, highlightedGroup)
      );
      return records;
    }
  }
}

async function getCelltypeBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  // Query fetching
  let query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
    a.aggregation("terms", "cell_type", { size: 1000 })
  );
  if (isSameLabel(label, highlightedGroup)) {
    query = query.filter("terms", "cell_type", highlightedGroup["value"]);
  } else if (highlightedGroup) {
    const cellIDs = await getCellIDs(dashboardID, highlightedGroup);
    query = query.filter("terms", "cell_id", cellIDs);
  }

  const results = await client.search({
    index: "dashboard_cells",
    body: query.build()
  });

  const getValue = isSameLabel(label, highlightedGroup)
    ? bucket => bucket["doc_count"]
    : bucket => bucket["agg_terms_cell_type"]["buckets"][0]["key"];

  return getDataMap(results, xBinSize, yBinSize, getValue);
}

async function getSampleBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  const sampleMap = await getSampleMap(dashboardID, label["label"]);

  // Query fetching
  let query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
    a.aggregation("terms", "sample_id", { size: 1000 })
  );
  if (isSameLabel(label, highlightedGroup)) {
    const sampleIDs = Object.keys(sampleMap).filter(
      sampleID => highlightedGroup["value"].indexOf(sampleMap[sampleID]) !== -1
    );

    query = query.filter("terms", "sample_id", sampleIDs);
  } else if (highlightedGroup) {
    const cellIDs = await getCellIDs(dashboardID, highlightedGroup);
    query = query.filter("terms", "cell_id", cellIDs);
  }

  const results = await client.search({
    index: "dashboard_cells",
    body: query.build()
  });

  const getValue = isSameLabel(label, highlightedGroup)
    ? bucket => bucket["doc_count"]
    : bucket =>
        getMajoritySample(bucket["agg_terms_sample_id"]["buckets"], sampleMap);

  return getDataMap(results, xBinSize, yBinSize, getValue);
}

export async function getSampleMap(dashboardID, label) {
  const sampleIDquery = bodybuilder()
    .size(1000)
    .filter("term", "patient_id", dashboardID)
    .filter("term", "type", "sample")
    .build();

  const sampleIDresults = await client.search({
    index: "dashboard_entry",
    body: sampleIDquery
  });

  const sampleIDMap = sampleIDresults["hits"]["hits"]
    .map(record => record["_source"])
    .reduce(
      (sampleMap, record) => ({
        ...sampleMap,
        [record["dashboard_id"]]: record[label][0]
      }),
      {}
    );

  return sampleIDMap;
}

const getMajoritySample = (buckets, sampleMap) => {
  const sampleCount = buckets.reduce((counts, bucket) => {
    const sample = sampleMap[bucket["key"]];

    return {
      ...counts,
      [sample]: counts.hasOwnProperty(sample)
        ? bucket["doc_count"] + counts[sample]
        : bucket["doc_count"]
    };
  }, {});

  return Object.keys(sampleCount).reduce(
    (currMax, sample) =>
      sampleCount[sample] > currMax["count"]
        ? { sample, count: sampleCount[sample] }
        : currMax,
    { sample: "", count: -1 }
  )["sample"];
};

async function getGeneBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  // Query fetching
  let query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
    a.aggregation("stats", "log_count")
  ).filter("term", "gene", label["label"]);

  if (isSameLabel(label, highlightedGroup)) {
    const [minGene, maxGene] = highlightedGroup["value"];

    query = query.filter("range", "log_count", {
      gte: minGene,
      lt: maxGene
    });
  } else if (highlightedGroup) {
    const cellIDs = await getCellIDs(dashboardID, highlightedGroup);
    query = query.filter("terms", "cell_id", cellIDs);
  }

  const results = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: query.build()
  });

  const getValue = isSameLabel(label, highlightedGroup)
    ? bucket => bucket["doc_count"]
    : bucket => bucket["agg_stats_log_count"]["sum"];

  return getDataMap(results, xBinSize, yBinSize, getValue);
}

async function getCellNumericalBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  // Query fetching
  let query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
    a.aggregation("percentiles", label["label"])
  );

  if (isSameLabel(label, highlightedGroup)) {
    const [minValue, maxValue] = highlightedGroup["value"];

    query = query.filter("range", label["label"], {
      gte: minValue,
      lt: maxValue === 1 ? 1.1 : maxValue
    });
  } else if (highlightedGroup) {
    const cellIDs = await getCellIDs(dashboardID, highlightedGroup);
    query = query.filter("terms", "cell_id", cellIDs);
  }

  const results = await client.search({
    index: "dashboard_cells",
    body: query.build()
  });

  const getValue = isSameLabel(label, highlightedGroup)
    ? bucket => bucket["doc_count"]
    : bucket => bucket[`agg_percentiles_${label["label"]}`]["values"]["50.0"];

  return getDataMap(results, xBinSize, yBinSize, getValue);
}

const isSameLabel = (label, highlightedGroup) =>
  highlightedGroup && label["label"] === highlightedGroup["label"];

export async function getCellIDs(dashboardID, highlightedGroup) {
  if (highlightedGroup["type"] === "CELL") {
    const baseQuery = bodybuilder()
      .size(50000)
      .filter("term", "dashboard_id", dashboardID);

    const query = highlightedGroup["isNum"]
      ? baseQuery
          .filter("range", highlightedGroup["label"], {
            gte: highlightedGroup["value"][0],
            lt:
              highlightedGroup["value"][1] === 1
                ? 1.1
                : highlightedGroup["value"][1]
          })
          .build()
      : baseQuery
          .filter(
            "terms",
            highlightedGroup["label"] === "celltype"
              ? "cell_type"
              : highlightedGroup["label"],
            highlightedGroup["value"]
          )
          .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["hits"]["hits"].map(record => record["_source"]["cell_id"]);
  } else if (highlightedGroup["type"] === "SAMPLE") {
    const sampleIDQuery = bodybuilder()
      .size(1000)
      .filter("term", "patient_id", dashboardID)
      .filter("terms", highlightedGroup["label"], highlightedGroup["value"])
      .filter("term", "type", "sample")
      .build();

    const sampleIDResults = await client.search({
      index: "dashboard_entry",
      body: sampleIDQuery
    });

    const sampleIDs = sampleIDResults["hits"]["hits"].reduce(
      (sampleIDs, record) => [...sampleIDs, ...record["_source"]["sample_ids"]],
      []
    );

    const query = bodybuilder()
      .size(50000)
      .filter("term", "dashboard_id", dashboardID)
      .filter("terms", "sample_id", sampleIDs)
      .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["hits"]["hits"].map(record => record["_source"]["cell_id"]);
  } else {
    // is "GENE"
    const query = bodybuilder()
      .size(0)
      .filter("term", "gene", highlightedGroup["label"])
      .filter("range", "log_count", {
        gte: highlightedGroup["value"][0],
        lt: highlightedGroup["value"][1]
      })
      .aggregation("terms", "cell_id", { size: 50000 })
      .build();

    const results = await client.search({
      index: `dashboard_genes_${dashboardID.toLowerCase()}`,
      body: query
    });

    return results["aggregations"]["agg_terms_cell_id"]["buckets"].map(
      bucket => bucket["key"]
    );
  }
}

export async function getBinSizes(dashboardID) {
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

  return { xBinSize, yBinSize };
}

export const getBaseDensityQuery = (
  dashboardID,
  xBinSize,
  yBinSize,
  labelAgg
) =>
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

export async function getAllBins(dashboardID, xBinSize, yBinSize) {
  const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
    a.aggregation("terms", "cell_type", { size: 1000 })
  );
  const results = await client.search({
    index: "dashboard_cells",
    body: query.build()
  });

  return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
    (records, xBucket) => [
      ...records,
      ...processXBuckets(
        xBucket,
        xBinSize,
        yBinSize,
        "total",
        yBucket => yBucket["doc_count"]
      )
    ],
    []
  );
}

const processXBuckets = (xBucket, xBinSize, yBinSize, label, getValue) =>
  xBucket["agg_histogram_y"]["buckets"].map(yBucket => {
    return {
      x: Math.round(xBucket["key"] / xBinSize),
      y: Math.round(yBucket["key"] / yBinSize),
      value: getValue(yBucket),
      label
    };
  });

export const getDataMap = (results, xBinSize, yBinSize, getValue) => {
  const processYBuckets = yBuckets =>
    yBuckets.reduce(
      (yMap, bucket) => ({
        ...yMap,
        [Math.round(bucket["key"] / yBinSize)]: getValue(bucket)
      }),
      {}
    );

  return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
    (dataMap, xBucket) => ({
      ...dataMap,
      [Math.round(xBucket["key"] / xBinSize)]: processYBuckets(
        xBucket["agg_histogram_y"]["buckets"]
      )
    }),
    {}
  );
};

export async function getRecords(
  dataMap,
  dashboardID,
  xBinSize,
  yBinSize,
  label,
  isDensity
) {
  const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);

  return allBins.map(record => {
    const { x, y, value } = record;

    return {
      x,
      y,
      label,
      value:
        dataMap.hasOwnProperty(x) && dataMap[x].hasOwnProperty(y)
          ? isDensity
            ? dataMap[x][y] / value
            : dataMap[x][y]
          : ""
    };
  });
}
