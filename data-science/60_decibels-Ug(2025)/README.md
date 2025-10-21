# SunCulture Data Science – 60 Decibels Uganda (2025)
 The script aims at getting an unbiased survey sample to be used by  60 Decibels to conduct an impact report through a structured survey in Uganda. The Sample selection will focus on impact metrics, particularly those related to carbon verification requirements,

This repository contains a Python-based data processing and Bayesian sampling workflow for the **60 Decibels Uganda 2025** dataset.  
The script `sample.py` automates data cleaning, exploratory analysis, and stratified sample size planning using Bayesian principles.

---


---

## 🚀 Overview

`sample.py` performs the following steps:

1. **Install and import dependencies**  
   Ensures all required Python libraries are available.

2. **Set working directory**  
   Automatically sets the working directory to the folder containing the script.

3. **Load dataset**  
   Reads the input Excel file (`data.xlsx`).

4. **Explore and clean data**  
   - Selects relevant columns  
   - Converts dates  
   - Calculates age  
   - Removes incomplete records

5. **Handle missing data**  
   - Fills missing `Age` values using the median per `District`  
   - Drops rows with any remaining missing `Age`

6. **Analyze and visualize**  
   Generates multiple plots showing:
   - Age distribution  
   - Sales per region, gender, product, and account type

7. **Bayesian sample size estimation & stratified sampling**  
   - Uses a Beta-Binomial posterior to estimate minimal sample size  
   - Allocates samples proportionally across strata (Region, Gender, Account Type, Product)  
   - Randomly selects customers from each stratum  
   - Exports final sample to Excel

---

## 🧰 Requirements

### ✅ Python version
- Python 3.8 or higher

### ✅ Required packages
The script will auto-install these if missing:


