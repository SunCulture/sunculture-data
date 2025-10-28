require("dotenv").config(); // Load variables from .env
const axios = require("axios");

// Sample list of lists
const dataList = [
{'accountRef':'28073760','paymentRef':'TJPPI87YCX','payerName':'Pay Bill from 25472****487 - JOSEPH **** KITENGU Acc. 28 073760','paymentAmount':'7000','timeStampMade':'2025-10-25 04:11:37','payerNumber':'25472****487 - JOSEPH **** KITENGU','paymentTypeId':1,'sourceAmountCurrency':'7000','selectedCurrency':{'id':1},'selectedBank':{'id':'15'}}
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

