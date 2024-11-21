const { MongoClient, ObjectId } = require("mongodb");
const path = require("path");
const fs = require("fs");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);
const dbName = "boonus";
const collectionName = "transactions";
const filePath = path.join(__dirname, "CustomerRewardsReport-5.csv");

const pipeline = [
  {
    $match: {
      businessId: new ObjectId("637da7ba3ecf30001fe918b7"),
      customerId: { $ne: new ObjectId("6655b275fc6868001deef307") },
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
      customerId: {
        $ifNull: [{ $arrayElemAt: ["$customerInfo._id", 0] }, "N/A"],
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
      lastVisitDate: {
        $ifNull: [
          {
            $dateToString: {
              format: "%d-%m-%Y", // Format: DD-MM-YYYY
              date: { $arrayElemAt: ["$customerLoyaltyInfo.lastVisit", 0] },
            },
          },
          "N/A",
        ],
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
      currentLevel: {
        $ifNull: [
          { $arrayElemAt: ["$customerLoyaltyInfo.currentLevel", 0] },
          "N/A",
        ],
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
      totalVisits: { $sum: 1 },
      //   totalVisits: {
      //     $sum: { $cond: [{ $eq: ["$type", "VISIT"] }, 1, 0] },
      //   },
      totalSpending: { $sum: "$amount" }, // Assuming "amount" represents spending
      totalNumberOfStamps: {
        $reduce: {
          input: {
            $map: {
              input: { $ifNull: ["$customerLoyaltyInfo.stampsProgress", []] }, // Ensure it's an array
              as: "item",
              in: {
                $ifNull: [
                  { $toInt: "$$item.numberOfStamps" }, // Apply $toInt to each element
                  0, // Default to 0 if the value is not valid
                ],
              },
            },
          },
          initialValue: 0,
          in: { $add: ["$$value", "$$this"] },
        },
      },
      currentStamps: {
        $size: {
          $filter: {
            input: {
              $ifNull: ["$serialNumbers", []],
            },
            as: "serial",
            cond: { $eq: ["$$serial.type", "STAMPS"] },
          },
        },
      },
      totalStampRewardsClaimed: {
        $sum: {
          $cond: [
            {
              $eq: [
                { $ifNull: ["$customerVouchersInfo.redeemed", false] },
                true,
              ],
            },
            1,
            0,
          ],
        },
      },
      totalRedeemedStampRewards: {
        $sum: {
          $cond: [
            {
              $eq: [
                { $ifNull: ["$customerVouchersInfo.type", null] },
                "STAMPS",
              ],
            },
            1,
            0,
          ],
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
              customerId: "$customerId",
              transactionType: "$type",
            },
            transactionDate: { $first: "$transactionDate" },
            customerId: { $first: "$customerId" },
            customerJoiningDate: { $first: "$customerJoiningDate" },
            lastVisitDate: { $first: "$lastVisitDate" },
            firstName: { $first: "$firstName" },
            lastName: { $first: "$lastName" },
            birthday: { $first: "$birthday" },
            gender: { $first: "$gender" },
            discount: { $first: "$discountAmount" },
            countryCode: { $first: "$countryCode" },
            contactNumber: { $first: "$contactNumber" },
            currentLevel: { $first: "$currentLevel" },
            currentPoints: { $first: "$currentPoints" },
            totalPoints: { $first: "$totalPoints" },
            totalRewardsClaimed: { $sum: "$totalRewardsCount" },
            totalNumberOfStamps: { $first: "$totalNumberOfStamps" },
            redeemedRewards: { $sum: "$redeemedRewardsCount" },
            totalVisits: { $sum: 1 },
            totalSpending: { $sum: "$amount" }, // Assuming "amount" represents spending
            transactionType: { $first: "$transactionType" },
            totalStampsReached: { $first: "$totalStampsReached" },
            currentStamps: { $first: "$currentStamps" },
            totalRedeemedStampRewards: { $first: "$totalRedeemedStampRewards" },
            totalStampRewardsClaimed: { $first: "$totalStampRewardsClaimed" },
          },
        },
        {
          $project: {
            _id: 0,
            transactionType: "$_id.transactionType",
            orderNumber: 1,
            businessType: 1,
            customerId: 1,
            customerJoiningDate: 1,
            lastVisitDate: 1,
            firstName: 1,
            lastName: 1,
            birthday: 1,
            gender: 1,
            countryCode: 1,
            discount: 1,
            contactNumber: 1,
            currentLevel: 1,
            currentPoints: 1,
            totalPoints: 1,
            totalRewardsClaimed: 1,
            redeemedRewards: 1,
            totalVisits: 1,
            totalSpending: 1,
            totalStampsReached: 1,
            currentStamps: 1,
            totalNumberOfStamps: 1,
            totalRedeemedStampRewards: 1,
            totalStampRewardsClaimed: 1,
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
  { $limit: 20 },
];

async function runAggregationAndSaveToCSV() {
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Create write stream
    const writeStream = fs.createWriteStream(filePath, { flags: "a" });
    writeStream.write(
      "First Name,Last Name,Birthday,Gender,Country Code,Contact Number,Joined At,Total Number of Stamps Reached,Current No. of Stamps,Total Stamp Rewards Claimed,Total Redeemed Stamp Rewards,Total Visits,Total Spending\n"
    );

    // Stream results in batches
    const cursor = collection.aggregate(pipeline, {
      allowDiskUse: true,
      batchSize: 10000,
    });

    // Await for cursor to process each batch
    await cursor.forEach((item) => {
      // Map fields from aggregation result to CSV row format
      const row = `${item.firstName},${item.lastName},${item.birthday},${item.gender},${item.countryCode},${item.contactNumber},${item.customerJoiningDate},${item.totalNumberOfStamps},${item.currentStamps},${item.totalStampRewardsClaimed},${item.totalRedeemedStampRewards},${item.totalVisits},${item.totalSpending}\n`;

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

runAggregationAndLog();

// runAggregationAndSaveToCSV();
