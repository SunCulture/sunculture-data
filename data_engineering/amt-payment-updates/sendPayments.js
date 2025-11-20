require("dotenv").config(); // Load variables from .env
const axios = require("axios");

// Sample list of lists
const dataList = [
  {
    'accountRef': '',
    'paymentRef': '',
    'payerName': '',
    'paymentAmount': '',
    'timeStampMade': '',
    'payerNumber': '',
    'paymentTypeId': null, // Numeric ID required
    'sourceAmountCurrency': '',
    'selectedCurrency': { id: null }, // Numeric ID required
    'selectedBank': { id: '' }
  }
]

// Iterate over the list of lists and send Axios requests
async function runPut() {
  for (let i = 0; i < dataList.length; i++) {
    let dataItem = dataList[i];

    async function send(dataItem) {
      // Create data string with dynamic values
      let data = dataItem;

      let config = {
        method: "POST",
        maxBodyLength: Infinity,
        url: process.env.API_URL, // ✅ Using .env instead of hardcoded URL
        headers: {
          api_key: process.env.API_KEY, // ✅ Using .env instead of hardcoded key
          "Content-Type": "application/json",
        },
        data: data,
      };

      axios
        .request(config)
        .then((response) => {
          console.log(
            `Request successful for ${data.paymentRef}': ${JSON.stringify(
              response.data
            )},`
          );
        })
        .catch((error) => {
          console.log(`Error: ${error},`);
        });
    }

    console.log(`Processing ${i + 1},  of ${dataList.length}, items`);
    await send(dataItem);
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

runPut().then(() => {});

