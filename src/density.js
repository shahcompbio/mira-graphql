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
    attributeCounts(
      dashboardID: String!
      label: AttributeInput!
      highlightedGroup: AttributeInput
    ): [AttributeValue!]

    attributes(dashboardType: String!, dashboardID: String!): [Attribute!]!
  }

  type DensityBin {
    x: Float!
    y: Float!
    label: String!
    value: StringOrNum!
  }

  type Attribute {
    isNum: Boolean!
    type: String!
    label: String!
  }

  type AttributeValue {
    isNum: Boolean!
    type: String!
    label: String!
    value: StringOrNum!
    count: Int!
  }

  input AttributeInput {
    isNum: Boolean!
    type: String!
    label: String!
    value: StringOrNum
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
    },

    async attributes(_, { dashboardType, dashboardID }) {
      // PURPOSE: Get all possible coloring values for UMAP

      // Categorical values
      const CELL_CATEGORICAL = ["celltype"].map(label => ({
        isNum: false,
        type: "CELL",
        label
      }));

      const SAMPLE_CATEGORICAL = [
        "surgery",
        "site",
        "treatment"
      ].map(label => ({ label, isNum: false, type: "SAMPLE" }));

      // Numerical values
      const cellFields = await client.indices.getMapping({
        index: "dashboard_cells"
      });

      const CELL_NUMERICAL = Object.keys(
        cellFields["dashboard_cells"]["mappings"]["properties"]
      )
        .filter(
          field =>
            ![
              "dashboard_id",
              "cell_id",
              "cell_type",
              "x",
              "y",
              "sample_id"
            ].includes(field)
        )
        .map(label => ({ isNum: true, label, type: "CELL" }));

      const geneQuery = bodybuilder()
        .size(0)
        .agg("terms", "gene", { size: 50000, order: { _key: "asc" } })
        .build();

      const geneResults = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: geneQuery
      });

      const GENE_NUMERICAL = geneResults["aggregations"]["agg_terms_gene"][
        "buckets"
      ].map(bucket => ({ isNum: true, label: bucket["key"], type: "GENE" }));

      return [
        ...CELL_CATEGORICAL,
        ...(dashboardType.toLowerCase() === "sample" ? [] : SAMPLE_CATEGORICAL),
        ...CELL_NUMERICAL,
        ...GENE_NUMERICAL
      ];
    },

    async attributeCounts(_, { dashboardID, label, highlightedGroup }) {
      if (label["isNum"]) {
        if (label["type"] === "CELL") {
          return await getCellNumericalCounts(
            dashboardID,
            label,
            highlightedGroup
          );
        } else {
          // is "GENE"

          return await getGeneExpressionCounts(
            dashboardID,
            label,
            highlightedGroup
          );
        }
      } else {
        // is categorical
        if (label["type"] === "CELL") {
          return await getCelltypeCounts(dashboardID, label, highlightedGroup);
        } else {
          return await getSampleCounts(dashboardID, label, highlightedGroup);
        }
      }
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
        isSameLabel(label, highlightedGroup)
          ? highlightedGroup["value"]
          : label["label"],
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
        isSameLabel(label, highlightedGroup)
          ? highlightedGroup["value"]
          : label["label"],
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
        isSameLabel(label, highlightedGroup)
          ? highlightedGroup["value"]
          : label["label"],
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
        isSameLabel(label, highlightedGroup)
          ? highlightedGroup["value"]
          : label["label"],
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
    query = query.filter("term", "cell_type", highlightedGroup["value"]);
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
      sampleID => sampleMap[sampleID] === highlightedGroup["value"]
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

async function getSampleMap(dashboardID, label) {
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
    const [minGene, maxGene] = highlightedGroup["value"].split("-");

    query = query.filter("range", "log_count", {
      gte: minGene.trim(),
      lt: maxGene.trim()
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
    const [minValue, maxValue] = highlightedGroup["value"].split("-");

    query = query.filter("range", label["label"], {
      gte: minValue.trim(),
      lt: parseFloat(maxValue.trim()) === 1 ? "1.1" : maxValue
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

async function getCellIDs(dashboardID, highlightedGroup) {
  if (highlightedGroup["type"] === "CELL") {
    const baseQuery = bodybuilder()
      .size(50000)
      .filter("term", "dashboard_id", dashboardID);

    const query = highlightedGroup["isNum"]
      ? baseQuery
          .filter("range", highlightedGroup["label"], {
            gte: highlightedGroup["value"].split("-")[0].trim(),
            lt:
              parseFloat(highlightedGroup["value"].split("-")[1].trim()) === 1
                ? "1.1"
                : highlightedGroup["value"].split("-")[1].trim()
          })
          .build()
      : baseQuery
          .filter(
            "term",
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
      .filter("term", highlightedGroup["label"], highlightedGroup["value"])
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
        gte: highlightedGroup["value"].split("-")[0].trim(),
        lt: highlightedGroup["value"].split("-")[1].trim()
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

async function getCelltypeCounts(dashboardID, label, highlightedGroup) {
  const celltypeQuery = bodybuilder()
    .size(0)
    .aggregation("terms", "celltype", { size: 50 })
    .build();

  const celltypeResults = await client.search({
    index: "rho_markers",
    body: celltypeQuery
  });

  const celltypes = [
    ...celltypeResults["aggregations"]["agg_terms_celltype"]["buckets"]
      .map(bucket => bucket["key"])
      .sort(),
    "Other"
  ];

  const cellIDs = !highlightedGroup
    ? []
    : await getCellIDs(dashboardID, highlightedGroup);

  const query = !highlightedGroup
    ? bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("terms", "cell_type", { size: 1000 })
        .build()
    : bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .filter("terms", "cell_id", cellIDs)
        .aggregation("terms", "cell_type", { size: 1000 })
        .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  const counts = results["aggregations"]["agg_terms_cell_type"][
    "buckets"
  ].reduce(
    (countsMap, bucket) => ({
      ...countsMap,
      [bucket["key"]]: bucket["doc_count"]
    }),
    {}
  );

  return celltypes.map(record => ({
    ...label,
    value: record,
    count: counts.hasOwnProperty(record) ? counts[record] : 0
  }));
}

async function getSampleCounts(dashboardID, label, highlightedGroup) {
  const sampleIDquery = bodybuilder()
    .size(1000)
    .filter("term", "patient_id", dashboardID)
    .filter("term", "type", "sample")
    .aggregation("terms", label["label"], { size: 1000 })
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
        [record["dashboard_id"]]: record[label["label"]][0]
      }),
      {}
    );

  const sampleCounts = sampleIDresults["aggregations"][
    `agg_terms_${label["label"]}`
  ]["buckets"]
    .map(bucket => bucket["key"])
    .sort();

  const cellIDs = !highlightedGroup
    ? []
    : await getCellIDs(dashboardID, highlightedGroup);

  const query = !highlightedGroup
    ? bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("terms", "sample_id", { size: 1000 })
        .build()
    : bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .filter("terms", "cell_id", cellIDs)
        .aggregation("terms", "sample_id", { size: 1000 })
        .build();

  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  const counts = results["aggregations"]["agg_terms_sample_id"][
    "buckets"
  ].reduce((countsMap, bucket) => {
    const sampleKey = sampleIDMap[bucket["key"]];

    return {
      ...countsMap,
      [sampleKey]: countsMap.hasOwnProperty(sampleKey)
        ? countsMap[sampleKey] + bucket["doc_count"]
        : bucket["doc_count"]
    };
  }, {});

  return sampleCounts.map(record => ({
    ...label,
    value: record,
    count: counts.hasOwnProperty(record) ? counts[record] : 0
  }));
}

async function getCellNumericalCounts(dashboardID, label, highlightedGroup) {
  const cellIDs = !highlightedGroup
    ? []
    : await getCellIDs(dashboardID, highlightedGroup);

  const query = !highlightedGroup
    ? bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("histogram", label["label"], {
          interval: 0.1,
          extended_bounds: { min: 0, max: 1 }
        })
        .build()
    : bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .filter("terms", "cell_id", cellIDs)
        .aggregation("histogram", label["label"], {
          interval: 0.1,
          extended_bounds: { min: 0, max: 1 }
        })
        .build();
  const results = await client.search({
    index: "dashboard_cells",
    body: query
  });

  const records = results["aggregations"][`agg_histogram_${label["label"]}`][
    "buckets"
  ].map(bucket => ({
    ...label,
    value: bucket["key"],
    count: bucket["doc_count"]
  }));

  const lastRecord = {
    ...records[records.length - 2],
    count:
      records[records.length - 2]["count"] +
      records[records.length - 1]["count"]
  };

  return [...records.slice(0, records.length - 2), lastRecord];
}

async function getGeneExpressionCounts(dashboardID, label, highlightedGroup) {
  const minMaxQuery = bodybuilder()
    .size(0)
    .filter("term", "gene", label["label"])
    .aggregation("stats", "log_count")
    .build();

  const minMaxResults = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: minMaxQuery
  });

  const min = minMaxResults["aggregations"]["agg_stats_log_count"]["min"];
  const max = minMaxResults["aggregations"]["agg_stats_log_count"]["max"];
  const binSize = (max - min) / 10;

  const cellIDs = !highlightedGroup
    ? []
    : await getCellIDs(dashboardID, highlightedGroup);

  const query = !highlightedGroup
    ? bodybuilder()
        .size(0)
        .filter("term", "gene", label["label"])
        .aggregation("histogram", "log_count", {
          interval: binSize,
          extended_bounds: { min, max }
        })
        .build()
    : bodybuilder()
        .size(0)
        .filter("terms", "cell_id", cellIDs)
        .filter("term", "gene", label["label"])
        .aggregation("histogram", "log_count", {
          interval: binSize,
          extended_bounds: { min, max }
        })
        .build();
  const results = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: query
  });

  const records = results["aggregations"][`agg_histogram_log_count`][
    "buckets"
  ].map(bucket => ({
    ...label,
    value: bucket["key"],
    count: bucket["doc_count"]
  }));

  const lastRecord = {
    ...records[records.length - 2],
    count:
      records[records.length - 2]["count"] +
      records[records.length - 1]["count"]
  };

  return [...records.slice(0, records.length - 2), lastRecord];
}

// ========================

async function getData(dashboardID, label, xBinSize, yBinSize, addFilters) {
  if (label["isNum"]) {
    if (label["type"] === "CELL") {
      const valueAgg = a => a.aggregation("percentiles", label["label"]);
      const query = addFilters(
        getBaseDensityQuery(dashboardID, xBinSize, yBinSize, valueAgg)
      );

      const results = await client.search({
        index: "dashboard_cells",
        body: query
      });
    } else {
      // is "GENE"
      const valueAgg = a => a.aggregation("stats", "log_count");
      const query = addFilters(
        getBaseDensityQuery(dashboardID, xBinSize, yBinSize, valueAgg)
      ).filter("term", "gene", label["label"]);

      const results = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: query.build()
      });
    }
  } else {
    // is categorical
    if (label["type"] === "CELL") {
      const valueAgg = a => a.aggregation("terms", "cell_type", { size: 1000 });
      const query = addFilters(
        getBaseDensityQuery(dashboardID, xBinSize, yBinSize, valueAgg)
      );

      const results = await client.search({
        index: "dashboard_cells",
        body: query.build()
      });
    } else {
      const valueAgg = a => a.aggregation("terms", "sample_id", { size: 1000 });
      const query = addFilters(
        getBaseDensityQuery(dashboardID, xBinSize, yBinSize, valueAgg)
      );

      const results = await client.search({
        index: "dashboard_cells",
        body: query.build()
      });
    }
  }
}

async function getBinSizes(dashboardID) {
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

async function getAllBins(dashboardID, xBinSize, yBinSize) {
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

const getDataMap = (results, xBinSize, yBinSize, getValue) => {
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

async function getRecords(
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
