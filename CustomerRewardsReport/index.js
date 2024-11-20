const { MongoClient, ObjectId } = require("mongodb");
const path = require("path");
const fs = require("fs");

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);
const dbName = "boonus";
const collectionName = "transactions";
const filePath = path.join(__dirname, "CustomerRewardsReport.csv");

const month = {
  1: "January",
  2: "February",
  3: "March",
  4: "April",
  5: "May",
  6: "June",
  7: "July",
  8: "August",
  9: "September",
  10: "October",
  11: "November",
  12: "December",
};

const pipeline = [
  {
    $match: {
      businessId: new ObjectId("63513c0716d2f7001f211c37"),
      created_at: {
        $gte: new Date("2023-01-01T00:00:00Z"),
        $lt: new Date("2025-01-01T00:00:00Z"),
      },
    },
  },
  {
    $addFields: {
      year: { $year: "$created_at" },
      month: { $month: "$created_at" },
    },
  },
  {
    $lookup: {
      from: "customers",
      localField: "customerId",
      foreignField: "_id",
      as: "customerInfo",
    },
  },
  {
    $addFields: {
      fullName: {
        $cond: {
          if: {
            $and: [
              { $eq: [{ $arrayElemAt: ["$customerInfo.firstName", 0] }, null] },
              { $eq: [{ $arrayElemAt: ["$customerInfo.lastName", 0] }, null] },
            ],
          },
          then: "N/A",
          else: {
            $concat: [
              {
                $ifNull: [{ $arrayElemAt: ["$customerInfo.firstName", 0] }, ""],
              },
              " ",
              {
                $ifNull: [{ $arrayElemAt: ["$customerInfo.lastName", 0] }, ""],
              },
            ],
          },
        },
      },
    },
  },
  {
    $facet: {
      groupData: [
        {
          $group: {
            _id: {
              year: "$year",
              month: "$month",
              customerId: "$customerId",
              rewardType: "$type", // Assuming "type" field indicates reward type
            },
            totalAmountRedeemed: { $sum: "$amount" },
            totalPoints: { $sum: "$points" },
            rewardsRedeemedCount: { $sum: 1 },
            fullName: { $first: "$fullName" },
          },
        },
        {
          $project: {
            _id: 0,
            year: "$_id.year",
            month: "$_id.month",
            customerId: "$_id.customerId",
            rewardType: "$_id.rewardType",
            fullName: 1,
            totalAmountRedeemed: 1,
            totalPoints: 1,
            rewardsRedeemedCount: 1,
          },
        },
      ],
      countRecords: [
        {
          $count: "totalRecords",
        },
      ],
    },
  },
  {
    $unwind: "$groupData",
  },
  {
    $sort: {
      "groupData.year": 1,
      "groupData.month": 1,
      "groupData.customerId": 1,
    },
  },
];

async function runAggregationAndSaveToCSV() {
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Create write stream
    const writeStream = fs.createWriteStream(filePath, { flags: "a" });
    writeStream.write(
      "Year,Month,Customer Id,Reward Type,Full Name,Total Amount Redeemed,TotalPoints,Rewards Redeemed Count\n"
    );

    // Stream results in batches
    const cursor = collection.aggregate(pipeline, {
      allowDiskUse: true,
      batchSize: 10000,
    });
    await cursor.forEach((item) => {
      const row = `${item.groupData.year},${month[item.groupData.month]},${
        item.groupData.customerId
      },${item.groupData.rewardType},${item.groupData.fullName},${
        item.groupData.totalAmountRedeemed
      },${item.groupData.totalPoints},${item.groupData.rewardsRedeemedCount}\n`;
      writeStream.write(row);
    });

    console.log(`Data successfully saved to ${filePath}`);
    writeStream.end();
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await client.close();
  }
}

runAggregationAndSaveToCSV();
