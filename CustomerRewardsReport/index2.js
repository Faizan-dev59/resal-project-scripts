const { MongoClient, ObjectId } = require("mongodb");
const path = require("path");
const fs = require("fs");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);
const dbName = "boonus";
const collectionName = "transactions";
const collectionName1 = "customerloyalties";
const filePath = path.join(__dirname, "CustomerRewardsReport-2.csv");

const pipeline1 = [
  {
    $match: {
      businessId: new ObjectId("6329ed73b92ed5001f7ae887"),
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
    $lookup: {
      from: "customerloyalties",
      localField: "customerLoyaltyId",
      foreignField: "_id",
      as: "customerLoyaltyInfo",
    },
  },
  {
    $addFields: {
      firstVoucherId: { $arrayElemAt: ["$customerVoucherIds", 0] }, // Extract the first element
    },
  },
  {
    $lookup: {
      from: "customervouchers", // Collection to join
      localField: "firstVoucherId", // Use the first element of the array
      foreignField: "_id", // Match on the _id field in customervouchers
      as: "customerVouchersInfo", // Resulting field with matched documents
    },
  },
  {
    $addFields: {
      customerId: {
        $ifNull: [{ $arrayElemAt: ["$customerInfo._id", 0] }, "N/A"],
      },
      firstName: {
        $ifNull: [{ $arrayElemAt: ["$customerInfo.firstName", 0] }, "N/A"],
      },
      lastName: {
        $ifNull: [{ $arrayElemAt: ["$customerInfo.lastName", 0] }, "N/A"],
      },
      birthday: {
        $ifNull: [
          {
            $dateToString: {
              format: "%d-%m-%Y", // Format: DD-MM-YYYY
              date: { $arrayElemAt: ["$customerInfo.birthDate", 0] },
            },
          },
          "N/A",
        ],
      },
      customerJoiningDate: {
        $ifNull: [
          {
            $dateToString: {
              format: "%d-%m-%Y", // Format: DD-MM-YYYY
              date: { $arrayElemAt: ["$customerInfo.created_at", 0] },
            },
          },
          "N/A",
        ],
      },
      gender: {
        $ifNull: [{ $arrayElemAt: ["$customerInfo.gender", 0] }, "N/A"],
      },
      countryCode: {
        $ifNull: [
          { $arrayElemAt: ["$customerInfo.phone.countryCode", 0] },
          "N/A",
        ],
      },
      contactNumber: {
        $ifNull: [{ $arrayElemAt: ["$customerInfo.phone.number", 0] }, "N/A"],
      },
      currentPoints: {
        $ifNull: [{ $arrayElemAt: ["$customerLoyaltyInfo.points", 0] }, "N/A"],
      },
      totalPoints: {
        $ifNull: [
          { $arrayElemAt: ["$customerLoyaltyInfo.totalPoints", 0] },
          "N/A",
        ],
      },
      branchName: {
        $ifNull: [{ $arrayElemAt: ["$branchInfo.name", 0] }, "N/A"],
      },
      cashierEmail: {
        $ifNull: [{ $arrayElemAt: ["$cashierInfo.email", 0] }, "N/A"],
      },
      customerVoucherId: {
        $ifNull: [{ $arrayElemAt: ["$customerVouchersInfo._id", 0] }, "N/A"],
      },
      redeemedRewardsCount: {
        $size: {
          $filter: {
            input: "$customerVouchersInfo",
            as: "voucher",
            cond: { $eq: ["$$voucher.redeemed", true] },
          },
        },
      },
      totalRewardsCount: { $sum: 1 },
    },
  },
  {
    $facet: {
      groupData: [
        {
          $group: {
            _id: "$customerId",
            customerId: { $first: "$customerId" },
            firstName: { $first: "$firstName" },
            lastName: { $first: "$lastName" },
            birthday: { $first: "$birthday" },
            customerJoiningDate: { $first: "$customerJoiningDate" },
            gender: { $first: "$gender" },
            countryCode: { $first: "$countryCode" },
            contactNumber: { $first: "$contactNumber" },
            currentPoints: { $first: "$currentPoints" },
            totalPoints: { $first: "$totalPoints" },
            totalRewardsClaimed: { $sum: "$totalRewardsCount" },
            redeemedRewards: { $sum: "$redeemedRewardsCount" },
            totalVisits: { $sum: 1 },
            totalSpending: { $sum: "$amount" }, // Assuming "amount" represents spending
            branchName: { $first: "$branchName" },
            cashierEmail: { $first: "$cashierEmail" },
          },
        },
        {
          $project: {
            _id: 0,
            customerId: 1,
            firstName: 1,
            lastName: 1,
            birthday: 1,
            customerJoiningDate: 1,
            gender: 1,
            countryCode: 1,
            contactNumber: 1,
            currentPoints: 1,
            totalPoints: 1,
            totalRewardsClaimed: 1,
            redeemedRewards: 1,
            totalVisits: 1,
            totalSpending: 1,
            branchName: 1,
            cashierEmail: 1,
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
    $replaceRoot: {
      newRoot: "$groupData",
    },
  },
  {
    $sort: { totalPoints: -1 }, // Example sort by total points
  },
];

const pipeline2 = [
  {
    $match: {
      businessId: new ObjectId("6329ed73b92ed5001f7ae887"), // Match the specific business ID
    },
  },
  {
    $lookup: {
      from: "customers", // Join with the customers collection
      localField: "customerId",
      foreignField: "_id",
      as: "customerInfo",
    },
  },
  {
    $addFields: {
      customerId: {
        $ifNull: [
          { $toString: { $arrayElemAt: ["$customerInfo._id", 0] } },
          "N/A",
        ], // Convert ObjectId to string
      },
      customerInfo: { $arrayElemAt: ["$customerInfo", 0] }, // Extract the first (and only) matched customer document
      birthday: {
        $ifNull: [
          {
            $dateToString: {
              format: "%d-%m-%Y", // Format: DD-MM-YYYY
              date: { $arrayElemAt: ["$customerInfo.birthDate", 0] },
            },
          },
          "N/A",
        ],
      },
      customerJoiningDate: {
        $ifNull: [
          {
            $dateToString: {
              format: "%d-%m-%Y", // Format: DD-MM-YYYY
              date: { $arrayElemAt: ["$customerInfo.created_at", 0] },
            },
          },
          "N/A",
        ],
      },
    },
  },
  {
    $project: {
      _id: 0, // Exclude the `_id` field from the output
      customerId: 1, // customerId is now a string due to $toString
      firstName: { $ifNull: ["$customerInfo.firstName", "N/A"] },
      lastName: { $ifNull: ["$customerInfo.lastName", "N/A"] },
      birthday: { $ifNull: ["$birthday", "N/A"] },
      gender: { $ifNull: ["$customerInfo.gender", "N/A"] },
      countryCode: { $ifNull: ["$customerInfo.phone.countryCode", "N/A"] },
      number: { $ifNull: ["$customerInfo.phone.number", "N/A"] },
      joinedAt: { $ifNull: ["$customerJoiningDate", "N/A"] },
      currentPoints: { $ifNull: ["$points", 0] },
      totalPoints: { $ifNull: ["$totalPoints", 0] },
    },
  },
];

async function runAggregationAndSaveToCSV() {
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const collection2 = db.collection(collectionName1);

    // Create write stream
    const writeStream = fs.createWriteStream(filePath, { flags: "a" });
    writeStream.write(
      "First Name,Last Name,Birthday,Gender,Country Code,Contact Number,Joined At,Current Points,Total Points,Total Rewards Claimed,Redemption Count,Total Visits,Total Spending\n"
    );

    // Run the aggregation and collect the results into an array for the first collection
    const results = await collection2
      .aggregate(pipeline2, {
        allowDiskUse: true,
        batchSize: 10000,
      })
      .toArray(); // Use toArray to fetch all the results

    // Run the aggregation for the second collection and collect the results
    const results2 = await collection
      .aggregate(pipeline1, {
        allowDiskUse: true,
        batchSize: 10000,
      })
      .toArray(); // Fetch all items from collection2

    // Map through the results and write to CSV
    results?.forEach((item, index) => {
      const pipelineResult2 = results2?.find(
        (res) => item?.customerId === res?.customerId.toString()
      );

      // Mapping fields from aggregation result to CSV row format
      const row = `${item.firstName},${item.lastName},${item.birthday},${
        item.gender
      },${item.countryCode},${item.number},${item.joinedAt},${
        item.currentPoints
      },${item.totalPoints},${pipelineResult2?.totalRewardsClaimed || 0},${
        pipelineResult2?.redeemedRewards || 0
      },${pipelineResult2?.totalVisits || 0},${
        pipelineResult2?.totalSpending || 0
      }\n`;

      // Write the row to the CSV
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

async function runAggregationAndLog() {
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const collection2 = db.collection(collectionName1);

    // Run the aggregation and collect the results into an array for the first collection
    const results = await collection2
      .aggregate(pipeline2, {
        allowDiskUse: true,
        batchSize: 10000,
      })
      .toArray(); // Use toArray to fetch all the results

    // // Log all items from collection2
    const results2 = await collection
      .aggregate(pipeline1, {
        allowDiskUse: true,
        batchSize: 10000,
      })
      .toArray(); // Fetch all items from collection2

    results?.map((item, index) => {
      const pipelineResult2 = results2?.find((res) => {
        item?.customerId === res?.customerId.toString();
      });
      console.log(`Record #${index + 1} from Collection:`);
      console.log({
        customerId: item?.customerId,
        firstName: item?.firstName,
        lastName: item?.lastName,
        birthday: item?.birthday,
        gender: item?.gender,
        countryCode: item?.countryCode,
        number: item?.number,
        joinedAt: item?.customerJoiningDate,
        currentPoints: item?.currentPoints,
        totalPoints: item?.totalPoints,
        totalRewardsClaimed: pipelineResult2?.totalRewardsClaimed || 0,
        redeemedRewards: pipelineResult2?.redeemedRewards || 0,
        totalVisits: pipelineResult2?.totalVisits || 0,
        totalSpending: pipelineResult2?.totalSpending || 0,
      });
      return {
        customerId: item?.customerId,
        firstName: item?.firstName,
        lastName: item?.lastName,
        birthday: item?.birthday,
        gender: item?.gender,
        countryCode: item?.countryCode,
        number: item?.number,
        joinedAt: item?.customerJoiningDate,
        currentPoints: item?.currentPoints,
        totalPoints: item?.totalPoints,
        totalRewardsClaimed: pipelineResult2?.totalRewardsClaimed || 0,
        redeemedRewards: pipelineResult2?.redeemedRewards || 0,
        totalVisits: pipelineResult2?.totalVisits || 0,
        totalSpending: pipelineResult2?.totalSpending || 0,
      };
    });

    console.log("Aggregation completed successfully.");
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await client.close();
  }
}

// runAggregationAndLog();

runAggregationAndSaveToCSV();
