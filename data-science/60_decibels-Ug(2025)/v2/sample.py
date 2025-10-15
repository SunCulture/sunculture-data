import importlib
import subprocess
import sys
from IPython.display import display

#Installing and importing required packages

def install_and_import_packages():
    """
    Installs and imports all required packages.
    """
    packages = {
        "pandas": "pd",
        "numpy": "np",
        "matplotlib.pyplot": "plt",
        "seaborn": "sns",
        "sklearn.preprocessing": "preprocessing",
        "sklearn.decomposition": "decomposition",
        "datetime": None,
        "os": None,
        "statsmodels":None
    }

    for package, alias in packages.items():
        try:
            importlib.import_module(package)
            print(f"✅ {package} is already installed.")
        except ImportError:
            base_pkg = package.split('.')[0]  # e.g. sklearn from sklearn.preprocessing
            print(f"📦 Installing {base_pkg} ...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", base_pkg])

    # Now import them into the global namespace
    globals().update({
        "pd": importlib.import_module("pandas"),
        "np": importlib.import_module("numpy"),
        "plt": importlib.import_module("matplotlib.pyplot"),
        "sns": importlib.import_module("seaborn"),
        "StandardScaler": importlib.import_module("sklearn.preprocessing").StandardScaler,
        "PCA": importlib.import_module("sklearn.decomposition").PCA,
        "datetime": importlib.import_module("datetime"),
        "os": importlib.import_module("os")
    })

    print("\n🎉 All required packages are installed and imported!")



# Automatically set working directory to this script's folder
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
print(f"📂 Working directory set to: {os.getcwd()}")


#File path to the dataset
file_path = r"C:\Users\USER\Documents\sunculture-data\data-science\60_decibels-Ug(2025)\data.xlsx"



#Reading, exploring, and cleaning the data

from pathlib import Path

def read_data(file_path):
    """
    Reads an Excel file into a pandas DataFrame.
    
    Args:
        file_path (str or Path): The path to the Excel file.
    
    Returns:
        pd.DataFrame: Loaded dataset.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"❌ File not found: {file_path}")
    
    df = pd.read_excel(file_path)
    print(f"✅ Successfully loaded file: {file_path}")
    print(f"🧾 Dataset shape: {df.shape}")
    return df


def explore_data(df):
    """
    Provides a quick overview of the dataset.
    
    Args:
        df (pd.DataFrame): DataFrame to explore.
    """
    print("🔍 First 5 rows:")
    display(df.head())

    print("\n📊 Column info:")
    print(df.info())

    print("\n🧮 Missing values per column:")
    print(df.isnull().sum())

    print("\n🔢 Basic statistics:")
    display(df.describe(include='all'))


def clean_data(df):
    """
    Cleans and processes the dataset:
      - Keeps only required columns
      - Converts Date_of_Birth to datetime
      - Calculates Age
      - Removes rows with nulls in required columns
    
    Args:
        df (pd.DataFrame): Raw dataset.
    
    Returns:
        pd.DataFrame: Cleaned dataset.
    """
    # Select only the required columns
    cols_to_keep = ['Customer_Id','Sale_Date','Product','Region',
                    'Account_type','Gender','Date_of_Birth','District','Units']
    
    df = df[cols_to_keep].copy()

    # Convert Date_of_Birth to datetime
    df['Date_of_Birth'] = pd.to_datetime(df['Date_of_Birth'], errors='coerce')

    # Compute Age
    current_year = datetime.datetime.now().year
    df['Age'] = current_year - df['Date_of_Birth'].dt.year

    # Drop rows with missing required fields
    required_cols = ["Customer_Id", "Sale_Date", "Product", "Region",
                     "Account_type", "Gender", "District", "Units"]
    
    df_clean = df.dropna(subset=required_cols)

    print(f"✅ Cleaned data: {df_clean.shape[0]} rows remaining after removing nulls.")
    return df_clean


#Visualising the data
def analyze_and_visualize_sales(df_clean):
    """
    Performs aggregation and visualization on the cleaned dataset.
    Generates multiple subplots showing customer distribution,
    units sold, and sales breakdowns by region, gender, account type, and product.

    Args:
        df_clean (pd.DataFrame): Cleaned dataset
    """
    # ---- Aggregations ----
    customers_region_gender = (
        df_clean.groupby(['Region', 'Gender'])['Customer_Id']
        .nunique()
        .reset_index()
    )

    sales_per_region = df_clean.groupby('Region')['Units'].sum()

    sales_per_account = df_clean.groupby('Account_type')['Units'].sum()
    sales_pct = (sales_per_account / sales_per_account.sum()) * 100

    sales_region_account = (
        df_clean.groupby(['Region', 'Account_type'])['Units']
        .sum()
        .reset_index()
    )

    sales_region_product = (
        df_clean.groupby(['Region', 'Product'])['Units']
        .sum()
        .reset_index()
    )

    # ---- Subplots Layout ----
    fig, axes = plt.subplots(3, 2, figsize=(16, 14))

    # Age Distribution
    sns.histplot(
        data=df_clean,
        x="Age",
        hue="Region",
        element="step",
        stat="density",
        common_norm=False,
        ax=axes[0, 0]
    )
    axes[0, 0].set_title('Age Distribution by Region')

    # Number of Customers per Region by Gender
    sns.barplot(
        data=customers_region_gender,
        x="Region",
        y="Customer_Id",
        hue="Gender",
        ax=axes[0, 1]
    )
    axes[0, 1].set_title('Number of Customers per Region by Gender')
    axes[0, 1].set_ylabel('Distinct Customers')

    # Add value labels
    for p in axes[0, 1].patches:
        height = p.get_height()
        axes[0, 1].text(
            p.get_x() + p.get_width() / 2.,
            height + 0.5,
            int(height),
            ha="center"
        )

    # Units Sold per Region
    sales_per_region.plot(kind='bar', ax=axes[1, 0], color='orange')
    axes[1, 0].set_title('Total Units Sold per Region')
    axes[1, 0].set_ylabel('Units')
    for i, v in enumerate(sales_per_region):
        axes[1, 0].text(i, v + 0.5, str(v), ha='center')

    # Percentage of Sales per Account Type (Pie Chart)
    sales_pct.plot(kind='pie', autopct='%1.1f%%', ax=axes[1, 1])
    axes[1, 1].set_title('Sales Percentage by Account Type')
    axes[1, 1].set_ylabel('')

    # Sales per Region by Account Type
    sns.barplot(
        data=sales_region_account,
        x="Region",
        y="Units",
        hue="Account_type",
        ax=axes[2, 0]
    )
    axes[2, 0].set_title('Sales per Region by Account Type')
    axes[2, 0].set_ylabel('Units')

    # Sales per Region by Product
    sns.barplot(
        data=sales_region_product,
        x="Region",
        y="Units",
        hue="Product",
        ax=axes[2, 1]
    )
    axes[2, 1].set_title('Sales per Region by Product')
    axes[2, 1].set_ylabel('Units')

    plt.tight_layout()
    plt.show()

    print("✅ Visualization complete.")


#Using Median to replace null values in the Age column
def fill_age_with_group_median(df_clean, group_col='District', target_col='Age'):
    """
    Fills missing values in the target column (e.g. 'Age') 
    using the median value of that column within each group (e.g. per 'District').

    Args:
        df (pd.DataFrame): Input DataFrame.
        group_col (str): Column name to group by (default 'District').
        target_col (str): Column whose nulls will be filled (default 'Age').

    Returns:
        pd.DataFrame: DataFrame with filled values in the target column.
    """
    # Make a copy to avoid modifying the original DataFrame directly
    df = df_clean.copy()

    # Fill missing values using median per group
    df[target_col] = df.groupby(group_col)[target_col].transform(
        lambda x: x.fillna(x.median())
    )

    print(f"✅ Missing '{target_col}' values filled using median by '{group_col}'.")
    return df


#Dropping rows with missing Age values
def drop_missing_age(df, age_col='Age'):
    """
    Drops rows where the Age column is null.
    
    Args:
        df (pd.DataFrame): Input DataFrame.
        age_col (str): Column name for age (default 'Age').

    Returns:
        pd.DataFrame: Cleaned DataFrame with missing ages removed.
    """
    initial_rows = len(df)
    df_final = df.dropna(subset=[age_col]).copy()
    removed = initial_rows - len(df_final)

    print(f"✅ Dropped {removed} rows with missing '{age_col}'.")
    print(f"Remaining rows: {len(df_final)}")
    return df_final


# Bayesian total sample size + proportional stratified allocation

from scipy.stats import beta as sp_beta

# ---------- 1. Posterior Width ----------
def beta_posterior_hdi_width(k, n, alpha=1, beta_param=1, hdi_prob=0.95):
    """
    Computes the 95% HDI (highest density interval) width 
    for a Beta-Binomial posterior given k successes in n trials.
    """
    a_post = alpha + k
    b_post = beta_param + n - k
    lower = sp_beta.ppf((1 - hdi_prob) / 2, a_post, b_post)
    upper = sp_beta.ppf(1 - (1 - hdi_prob) / 2, a_post, b_post)
    return upper - lower, (lower, upper)

# ---------- 2. Bayesian Sample Size Estimation ----------
def find_min_n_bayesian(p_assumed, target_width=0.10, max_n=5000, step=50,
                        alpha_prior=1, beta_prior=1, verbose=False):
    """
    Iteratively finds the minimal n such that the 95% posterior interval width
    is less than or equal to target_width.
    """
    for n in range(step, max_n + 1, step):
        k = int(round(p_assumed * n))
        width, ci = beta_posterior_hdi_width(
            k, n, alpha=alpha_prior, beta_param=beta_prior, hdi_prob=0.95
        )
        if verbose:
            print(f"n={n:5d}  k={k:4d}  CI width={width:.4f}  95% CI=({ci[0]:.4f},{ci[1]:.4f})")
        if width <= target_width:
            return n, k, width, ci
    return None, None, None, None

# ---------- 3. Stratified Proportional Allocation ----------
def stratified_allocation(frame, strata_cols, min_n, min_per_stratum=5):
    """
    Allocates a total sample size across strata proportionally to customer counts.
    Ensures a minimum number of samples per stratum and corrects rounding differences.
    """
    available_strata = [c for c in strata_cols if c in frame.columns]
    if not available_strata:
        raise RuntimeError("❌ No strata columns available in the frame.")

    strata_ser = frame.groupby(available_strata)['Customer_Id'].nunique()
    total_customers = strata_ser.sum()

    alloc = (strata_ser / total_customers * min_n).round().astype(int)

    # Fix rounding mismatch
    diff = int(min_n - alloc.sum())
    if diff != 0:
        idx = 0
        keys = list(alloc.index)
        while diff != 0:
            key = keys[idx % len(keys)]
            alloc.loc[key] += 1 if diff > 0 else -1
            diff = int(min_n - alloc.sum())
            idx += 1

    # Enforce minimum per stratum
    alloc = alloc.apply(lambda x: max(x, min_per_stratum))

    # Adjust downward if sum exceeds min_n
    while alloc.sum() > min_n:
        reducible = alloc[alloc > min_per_stratum].sort_values(ascending=False)
        if reducible.empty:
            break
        top = reducible.index[0]
        alloc.loc[top] -= 1

    alloc_df = alloc.reset_index()
    alloc_df.columns = available_strata + ['n_alloc']

    return alloc_df

# ---------- 4. Main Driver Function ----------
def bayesian_sample_planner(df_final, 
                            IndicatorCol=None, 
                            target_width=0.10, 
                            max_n=1854, 
                            step=50,
                            prior_alpha=1, 
                            prior_beta=1, 
                            strata_cols=None, 
                            min_per_stratum=5,
                            random_seed=2025, 
                            output_csv="bayes_stratified_allocation.csv",
                            verbose=False):
    """
    Full Bayesian sample size planning workflow:
      1. Detect indicator variable
      2. Estimate minimal Bayesian sample size
      3. Perform stratified proportional allocation
      4. Save results to CSV
    """
    np.random.seed(random_seed)

    # Copy DataFrame
    frame = df_final.copy()

    # ---- Indicator detection ----
    if IndicatorCol is None:
        if 'Units' in frame.columns:
            frame['_indicator'] = (frame['Units'].fillna(0) > 0).astype(int)
            IndicatorCol = '_indicator'
            if verbose: print("Auto-selected indicator: Units > 0")
        else:
            frame['_indicator'] = frame['Age'].notnull().astype(int)
            IndicatorCol = '_indicator'
            if verbose: print("Auto-selected indicator: Age.notnull() (fallback).")
    else:
        if IndicatorCol not in frame.columns:
            raise ValueError(f"IndicatorCol '{IndicatorCol}' not found in dataframe.")
        if frame[IndicatorCol].nunique() > 2:
            if frame[IndicatorCol].dtype.kind in 'biufc':
                frame['_indicator'] = (frame[IndicatorCol].fillna(0) > 0).astype(int)
                IndicatorCol = '_indicator'
            else:
                raise ValueError("Indicator column not binary.")
        else:
            vals = sorted(frame[IndicatorCol].dropna().unique())
            if set(vals) <= {0,1}:
                frame['_indicator'] = frame[IndicatorCol].astype(int)
                IndicatorCol = '_indicator'
            else:
                mapping = {vals[0]: 0, vals[-1]: 1}
                frame['_indicator'] = frame[IndicatorCol].map(mapping).astype(int)
                IndicatorCol = '_indicator'
                if verbose: print(f"Mapped {vals} to {mapping}.")

    # ---- Empirical baseline ----
    observed_p = frame[IndicatorCol].mean()
    N_pop = frame['Customer_Id'].nunique()
    print(f"Frame size (unique customers): {N_pop}, observed p̂ = {observed_p:.4f}")

    # ---- Bayesian sample size ----
    min_n, min_k, width, ci = find_min_n_bayesian(
        observed_p, target_width, max_n, step,
        alpha_prior=prior_alpha, beta_prior=prior_beta, verbose=verbose
    )
    if min_n is None:
        raise RuntimeError("❌ Could not find n that meets target_width. Increase max_n or relax target_width.")
    print(f"✅ Suggested n = {min_n}, 95% CI width = {width:.4f}, CI = ({ci[0]:.4f},{ci[1]:.4f})")

    # ---- Stratified allocation ----
    if strata_cols is None:
        strata_cols = ['Region', 'Gender', 'Account_type', 'Product']

    alloc_df = stratified_allocation(frame, strata_cols, min_n, min_per_stratum)
    print(f"\n📊 Stratified allocation (first 10 rows):\n{alloc_df.head(10)}")
    print(f"Total allocated = {alloc_df['n_alloc'].sum()}")

    alloc_df.to_csv(output_csv, index=False)
    print(f"✅ Allocation saved to '{output_csv}'")

    return alloc_df


# ---------- 5. Sample Selection Function ----------
def select_stratified_sample(frame, alloc_df, strata_cols, random_seed=2025,
                             output_excel="bayes_selected_sample.xlsx"):
    """
    Selects a stratified random sample based on an allocation table.

    Steps:
      1. Count available customers per stratum
      2. Merge counts into allocation table
      3. Cap allocations if they exceed available customers
      4. Randomly sample per stratum (without replacement)
      5. Combine and export the final sample to Excel

    Args:
        frame (pd.DataFrame): Full dataset (with 'Customer_Id' column)
        alloc_df (pd.DataFrame): Allocation table containing strata and n_alloc
        strata_cols (list): Columns to use for stratification
        random_seed (int): Random seed for reproducibility
        output_excel (str): File path to save the final sampled dataset

    Returns:
        pd.DataFrame: Final sampled dataset
    """
    # Step 1: Count available customers per stratum
    strata_counts = (
        frame.groupby(strata_cols)['Customer_Id']
        .nunique()
        .reset_index(name='N_available')
    )

    # Step 2: Merge available counts into allocation table
    alloc_df = alloc_df.merge(strata_counts, on=strata_cols, how='left')

    # Step 3: Cap allocations so they do not exceed available customers
    alloc_df['n_alloc'] = alloc_df[['n_alloc', 'N_available']].min(axis=1).astype(int)

    print("\n📊 Adjusted stratified allocation (first 10 rows):")
    print(alloc_df.head(10))
    print(f"\nAdjusted total sample size = {alloc_df['n_alloc'].sum()}")

    # Step 4: Select samples per stratum
    selected_samples = []
    for _, row in alloc_df.iterrows():
        subset = frame.copy()
        for col in strata_cols:
            subset = subset[subset[col] == row[col]]

        # Sample without replacement
        if len(subset) >= row['n_alloc']:
            sample = subset.sample(n=row['n_alloc'], random_state=random_seed)
        else:
            sample = subset  # if fewer available, take all
        selected_samples.append(sample)

    # Step 5: Combine all selected strata samples
    final_sample = pd.concat(selected_samples, ignore_index=True)

    # Save to Excel
    final_sample.to_excel(output_excel, index=False)

    print(f"\n✅ Final selected sample size = {final_sample['Customer_Id'].nunique()} unique customers")
    print(f"💾 Saved to '{output_excel}'")

    return final_sample




if __name__ == "__main__":
    # 1️⃣ Install and import required packages
    install_and_import_packages()

    # 2️⃣ Read the data
    df = read_data(file_path)

    # 3️⃣ Explore data
    explore_data(df)

    # 4️⃣ Clean data
    df_clean = clean_data(df)

    # 5️⃣ Fill missing ages by median per district
    df_filled = fill_age_with_group_median(df_clean)

    # 6️⃣ Drop any remaining null ages
    df_final = drop_missing_age(df_filled)

    # 7️⃣ Visualize results
    analyze_and_visualize_sales(df_final)

    # 8️⃣ Run Bayesian sample size and stratified allocation
    alloc_df = bayesian_sample_planner(df_final, verbose=True)

    # 9️⃣ Select the stratified sample and save it
    final_sample = select_stratified_sample(
        frame=df_final,
        alloc_df=alloc_df,
        strata_cols=['Region', 'Gender', 'Account_type', 'Product']
    )

    print("\n🎯 Script completed successfully!")
