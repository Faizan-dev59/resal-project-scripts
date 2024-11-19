const fs = require("fs");
const csv = require("csv-parser");
const axios = require("axios");

const enrollCustomers = async () => {
  const customers = [];
  const phoneNumbersSet = new Set(); // Set to track enrolled phone numbers

  // Read CSV file and extract data
  fs.createReadStream("merchant-file.csv") // Replace with your actual CSV file path
    .pipe(csv())
    .on("data", (row) => {
      const phoneNumber = row["رقم الجوال"];
      if (!phoneNumbersSet.has(phoneNumber)) {
        phoneNumbersSet.add(phoneNumber);
        customers.push({
          firstName: row["الاسم الأول"].trim(),
          lastName: row["الاسم الأخير "].trim(),
          phone: {
            countryCode: "966",
            number: phoneNumber,
          },
          points:
            row["مجموع عدد نقاط العميلة "].trim() >= 0
              ? Number(row["مجموع عدد نقاط العميلة "].trim())
              : 0,
        });
      } else {
        console.warn(
          `Duplicate phone number found: ${phoneNumber}. This entry will be skipped.`
        );
      }
    })
    .on("end", async () => {
      console.log("CSV file successfully processed. Enrolling customers...");
      await processCustomers(customers);
    });
};

const processCustomers = async (customers) => {
  const apiUrl =
    "http://localhost:5000/api/v1/buyer/auth/enroll-customer-wallet";
  const token =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJBcGlBdXRoIiwiX2lkIjoiNjcxNGZlZGEzYmEwMGYxYTI0MDMzOTA1Iiwic3ViIjoidGVzdEBnbWFpbC5jb20iLCJhZG1pbiI6ZmFsc2UsInJvbGUiOiJPV05FUiIsImZpcnN0VGltZSI6ZmFsc2UsImRlZmF1bHRCdXNpbmVzcyI6IjY3MTRmZWRhM2JhMDBmMWEyNDAzMzkwMyIsImlhdCI6MTczMDcxMjAwMTk4NSwiZXhwIjoxNzMxMzE2ODAxOTg1fQ.D7ovCKV5q7eYm44okIGy9rzBK5nTsVxKxa9nVuuRUxc"; // Replace with your actual token
  const batchSize = 15;
  const delayBetweenBatches = 500; // 1 second delay between batches

  const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  for (let i = 0; i < customers.length; i += batchSize) {
    const batchRequests = [];

    for (let j = 0; j < batchSize && i + j < customers.length; j++) {
      const customerData = {
        firstName: customers[i + j].firstName,
        lastName: customers[i + j].lastName,
        // gender: "MALE", // You may modify this based on your CSV data
        // birthDate: "2000-10-31T11:59:51.812Z",
        walletId: "67232e6111e02c5e58e87079",
        businessId: "6714feda3ba00f1a24033903",
        consentPermission: true,
        phone: customers[i + j].phone,
        points: customers[i + j].points,
        totalPoints: customers[i + j].points,
        language: "EN",
      };

      batchRequests.push(
        axios
          .post(apiUrl, customerData, {
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          })
          .then((response) => {
            console.log(`Customer ${i + j + 1} enrolled successfully.`);
            return response.data;
          })
          .catch((error) => {
            console.error(
              `Error enrolling Customer ${i + j + 1}:`,
              error.response ? error.response.data : error.message
            );
            return null; // Return null to keep the batch processing consistent
          })
      );
    }

    try {
      await Promise.all(batchRequests);
    } catch (err) {
      console.error(
        `Error processing batch starting with Customer ${i + 1}:`,
        err
      );
    }

    console.log(`Batch starting with Customer ${i + 1} completed.`);

    if (i + batchSize < customers.length) {
      await pause(delayBetweenBatches);
    }
  }
};

// Run the script
enrollCustomers();
