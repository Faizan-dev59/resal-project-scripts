const { MongoClient, ObjectId } = require("mongodb");
const path = require("path");
const fs = require("fs");

const uri =
  "mongodb://vt_admin:7A4J2wANFY6i@ec2-65-1-109-135.ap-south-1.compute.amazonaws.com:27017/boonus?directConnection=true";
const client = new MongoClient(uri);
const dbName = "boonus";
const collectionName = "transactions";
const filePath = path.join(__dirname, "CustomerRewardsReport-2.csv");

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

async function runAggregationAndSaveToCSV() {
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Create write stream
    const writeStream = fs.createWriteStream(filePath, { flags: "a" });
    writeStream.write(
      "First Name,Last Name,Birthday,Gender,Country Code,Contact Number,Joined At,Current Points,Total Points,Total Rewards Claimed,Redemption Count,Total Visits,Total Spending\n"
    );

    // Stream results in batches
    const cursor = collection.aggregate(pipeline, {
      allowDiskUse: true,
      batchSize: 10000,
    });

    // Await for cursor to process each batch
    await cursor.forEach((item) => {
      // Mapping fields from aggregation result to CSV row format
      const row = `${item.firstName},${item.lastName},${item.birthday},${item.gender},${item.countryCode},${item.contactNumber},${item.customerJoiningDate},${item.currentPoints},${item.totalPoints},${item.totalRewardsClaimed},${item.redeemedRewards},${item.totalVisits},${item.totalSpending}\n`;

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

    // Run the aggregation and collect the results into an array
    const results = await collection
      .aggregate(pipeline, {
        allowDiskUse: true,
        batchSize: 10000,
      })
      .toArray(); // Use toArray to fetch all the results

    console.log("Aggregation Pipeline Results:");
    // Log the aggregation results
    console.log(JSON.stringify(results, null, 2)); // This will print the results nicely formatted

    // You can also log specific fields for easier readability if you prefer:
    results.forEach((item, index) => {
      console.log(`Record #${index + 1}:`);
      console.log(item);
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
