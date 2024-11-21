const { MongoClient, ObjectId } = require("mongodb");
const path = require("path");
const fs = require("fs");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);
const dbName = "boonus";
const collectionName = "customerloyalties";
const filePath = path.join(__dirname, "CustomerGeneralReport-5.csv");

const pipeline = [
  {
    $match: {
      businessId: new ObjectId("637da7ba3ecf30001fe918b7"), // Match the specific business ID
      customerId: { $ne: new ObjectId("6655b275fc6868001deef307") },
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
      customerId: { $ifNull: ["$customerInfo._id", "N/A"] },
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

    // Create write stream
    const writeStream = fs.createWriteStream(filePath, { flags: "a" });
    writeStream.write(
      "ID,First Name,Last Name,Birthday,Gender,Country Code,Phone Number,Joined At,Current Points,Total Points\n"
    );

    // Stream results in batches
    const cursor = collection.aggregate(pipeline, {
      allowDiskUse: true,
      batchSize: 10000,
    });

    // Await for cursor to process each batch
    await cursor.forEach((item) => {
      // Mapping fields from aggregation result to CSV row format
      const row = `${item.customerId || "N/A"},${item.firstName || "N/A"},${
        item.lastName || "N/A"
      },${item.birthday || "N/A"},${item.gender || "N/A"},${
        item.countryCode || "N/A"
      },${item.number || "N/A"},${item.joinedAt || "N/A"},${
        item.currentPoints || 0
      },${item.totalPoints || 0}\n`;

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
